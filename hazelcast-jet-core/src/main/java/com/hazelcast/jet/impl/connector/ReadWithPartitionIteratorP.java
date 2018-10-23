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
import com.hazelcast.client.impl.clientside.HazelcastClientProxy;
import com.hazelcast.client.proxy.ClientMapProxy;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.MembershipAdapter;
import com.hazelcast.core.MembershipEvent;
import com.hazelcast.core.MigrationEvent;
import com.hazelcast.core.MigrationListener;
import com.hazelcast.core.Partition;
import com.hazelcast.jet.RestartableException;
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
import javax.annotation.Nullable;
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
    private final MigrationWatcher migrationWatcher;

    ReadWithPartitionIteratorP(
            Function<? super Integer, ? extends Iterator<T>> partitionToIterator,
            List<Integer> partitions,
            MigrationWatcher migrationWatcher
    ) {
        this.migrationWatcher = migrationWatcher;
        final CircularListCursor<Iterator<T>> iteratorCursor = new CircularListCursor<>(
                partitions.stream().map(partitionToIterator).collect(toList())
        );
        this.outputTraverser = () -> {
            do {
                final Iterator<T> currIterator = iteratorCursor.value();
                while (currIterator.hasNext()) {
                    T next = currIterator.next();
                    // iterator can return null element (entry projected to null), we ignore these
                    if (next != null) {
                        iteratorCursor.advance();
                        return next;
                    }
                }
                iteratorCursor.remove();
                checkMigration();
            } while (iteratorCursor.advance());
            return null;
        };
    }

    @Override
    public boolean isCooperative() {
        return false;
    }

    public static <T> ProcessorMetaSupplier readMapSupplier(@Nonnull String mapName) {
        return new LocalClusterMetaSupplier<T>(
                instance -> partition -> ((MapProxyImpl) instance.getMap(mapName))
                        .iterator(FETCH_SIZE, partition, PREFETCH_VALUES));
    }

    public static <T> ProcessorMetaSupplier readRemoteMapSupplier(
            @Nonnull String mapName, @Nonnull ClientConfig clientConfig
    ) {
        return new RemoteClusterMetaSupplier<T>(clientConfig,
                instance -> partition -> ((ClientMapProxy) instance.getMap(mapName))
                        .iterator(FETCH_SIZE, partition, PREFETCH_VALUES));
    }

    public static <K, V, T> ProcessorMetaSupplier readMapSupplier(
            @Nonnull String mapName,
            @Nonnull Predicate<? super K, ? super V> predicate,
            @Nonnull Projection<? super Map.Entry<K, V>, ? extends T> projection
    ) {
        checkSerializable(predicate, "predicate");
        checkSerializable(projection, "projection");

        return new LocalClusterMetaSupplier<T>(
                instance -> partition -> {
                    MapProxyImpl map = (MapProxyImpl) instance.getMap(mapName);
                    return map.iterator(FETCH_SIZE, partition, projection, predicate);
                });
    }

    public static <K, V, T> ProcessorMetaSupplier readRemoteMapSupplier(
            @Nonnull String mapName,
            @Nonnull ClientConfig clientConfig,
            @Nonnull Projection<? super Entry<K, V>, ? extends T> projection,
            @Nonnull Predicate<? super K, ? super V> predicate
    ) {
        checkSerializable(projection, "projection");
        checkSerializable(predicate, "predicate");

        return new RemoteClusterMetaSupplier<T>(clientConfig,
                instance -> partition -> ((ClientMapProxy) instance.getMap(mapName))
                        .iterator(FETCH_SIZE, partition, projection, predicate));
    }

    public static ProcessorMetaSupplier readCacheSupplier(@Nonnull String cacheName) {
        return new LocalClusterMetaSupplier<>(
                instance -> partition -> ((CacheProxy) instance.getCacheManager().getCache(cacheName))
                        .iterator(FETCH_SIZE, partition, PREFETCH_VALUES));
    }

    public static ProcessorMetaSupplier readRemoteCacheSupplier(
            @Nonnull String cacheName,
            @Nonnull ClientConfig clientConfig
    ) {
        return new RemoteClusterMetaSupplier<>(clientConfig,
                instance -> partition -> ((ClientCacheProxy) instance.getCacheManager().getCache(cacheName))
                        .iterator(FETCH_SIZE, partition, PREFETCH_VALUES));
    }

    @Override
    public boolean complete() {
        checkMigration();
        return emitFromTraverser(outputTraverser);
    }

    private void checkMigration() {
        if (migrationWatcher.clusterChanged()) {
            throw new RestartableException("Partition migration detected");
        }
    }

    private static <T> List<Processor> getProcessors(
            int count,
            List<Integer> ownedPartitions,
            Function<? super Integer, ? extends Iterator<T>> partitionToIterator,
            MigrationWatcher migrationWatcher
    ) {
        return processorToPartitions(count, ownedPartitions)
                .values().stream()
                .map(partitions -> !partitions.isEmpty()
                        ? new ReadWithPartitionIteratorP<>(partitionToIterator, partitions, migrationWatcher)
                        : Processors.noopP().get()
                )
                .collect(toList());
    }

    private static class RemoteClusterMetaSupplier<T> implements ProcessorMetaSupplier {

        static final long serialVersionUID = 1L;

        private final SerializableClientConfig serializableConfig;
        private final DistributedFunction<? super HazelcastInstance, ? extends Function<Integer, Iterator<T>>>
                iteratorSupplier;

        private transient int remotePartitionCount;

        RemoteClusterMetaSupplier(
            ClientConfig clientConfig,
            DistributedFunction<? super HazelcastInstance, ? extends Function<Integer, Iterator<T>>> iteratorSupplier
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
        private final DistributedFunction<? super HazelcastInstance, ? extends Function<Integer, Iterator<T>>>
                iteratorSupplier;

        private transient HazelcastInstance client;
        private transient Function<? super Integer, ? extends Iterator<T>> partitionToIterator;
        private transient MigrationWatcher migrationWatcher;

        RemoteClusterProcessorSupplier(
            List<Integer> ownedPartitions,
            SerializableClientConfig serializableClientConfig,
            DistributedFunction<? super HazelcastInstance, ? extends Function<Integer, Iterator<T>>> iteratorSupplier
        ) {
            this.ownedPartitions = ownedPartitions;
            this.serializableClientConfig = serializableClientConfig;
            this.iteratorSupplier = iteratorSupplier;
        }

        @Override
        public void init(@Nonnull Context context) {
            client = newHazelcastClient(serializableClientConfig.asClientConfig());
            migrationWatcher = new MigrationWatcher(client);
            partitionToIterator = iteratorSupplier.apply(client);
        }

        @Override
        public void close(Throwable error) {
            if (migrationWatcher != null) {
                migrationWatcher.deregister();
            }
            if (client != null) {
                client.shutdown();
            }
        }

        @Override @Nonnull
        public List<Processor> get(int count) {
            return getProcessors(count, ownedPartitions, partitionToIterator, migrationWatcher);
        }
    }

    private static class LocalClusterMetaSupplier<T> implements ProcessorMetaSupplier {

        static final long serialVersionUID = 1L;

        private final DistributedFunction<? super HazelcastInstance, ? extends Function<Integer, Iterator<T>>>
                iteratorSupplier;

        private transient Map<Address, List<Integer>> addrToPartitions;

        LocalClusterMetaSupplier(
            DistributedFunction<? super HazelcastInstance, ? extends Function<Integer, Iterator<T>>> iteratorSupplier
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
        private final DistributedFunction<? super HazelcastInstance, ? extends Function<Integer, Iterator<T>>>
                iteratorSupplier;

        private transient Function<Integer, Iterator<T>> partitionToIterator;
        private transient MigrationWatcher migrationWatcher;

        LocalClusterProcessorSupplier(
            List<Integer> ownedPartitions,
            DistributedFunction<? super HazelcastInstance, ? extends Function<Integer, Iterator<T>>> iteratorSupplier
        ) {
            this.ownedPartitions = ownedPartitions != null ? ownedPartitions : Collections.emptyList();
            this.iteratorSupplier = iteratorSupplier;
        }

        @Override
        public void init(@Nonnull Context context) {
            partitionToIterator = iteratorSupplier.apply(context.jetInstance().getHazelcastInstance());
            migrationWatcher = new MigrationWatcher(context.jetInstance().getHazelcastInstance());
        }

        @Override @Nonnull
        public List<Processor> get(int count) {
            return getProcessors(count, ownedPartitions, partitionToIterator, migrationWatcher);
        }

        @Override
        public void close(@Nullable Throwable error) {
            if (migrationWatcher != null) {
                migrationWatcher.deregister();
            }
        }
    }

    static class MigrationWatcher {

        private final HazelcastInstance instance;
        private final String membershipListenerReg;
        private final String partitionListenerReg;

        private volatile boolean clusterChanged;

        MigrationWatcher(HazelcastInstance instance) {
            this.instance = instance;

            membershipListenerReg = registerMembershipListener(instance);
            partitionListenerReg = registerMigrationListener(instance);
        }

        private String registerMembershipListener(HazelcastInstance instance) {
            return instance.getCluster().addMembershipListener(new MembershipAdapter() {
                @Override
                public void memberAdded(MembershipEvent membershipEvent) {
                    if (!membershipEvent.getMember().isLiteMember()) {
                        setChanged();
                    }
                }

                @Override
                public void memberRemoved(MembershipEvent membershipEvent) {
                    if (!membershipEvent.getMember().isLiteMember()) {
                        setChanged();
                    }
                }
            });
        }

        private String registerMigrationListener(HazelcastInstance instance) {
            try {
                return instance.getPartitionService().addMigrationListener(new MigrationListener() {
                    @Override
                    public void migrationStarted(MigrationEvent migrationEvent) {
                        // Note: this event is fired also when a partition is lost or if a split merge occurs
                        setChanged();
                    }

                    @Override
                    public void migrationCompleted(MigrationEvent migrationEvent) {
                        setChanged();
                    }

                    @Override
                    public void migrationFailed(MigrationEvent migrationEvent) {
                        setChanged();
                    }
                });
            } catch (UnsupportedOperationException e) {
                // MigrationListener is not supported on client
                return null;
            }
        }

        private void setChanged() {
            clusterChanged = true;
        }

        /**
         * Returns {@code true} if any partition migration or member addition and removal took place
         * since creation.
         */
        boolean clusterChanged() {
            return clusterChanged;
        }

        void deregister() {
            instance.getCluster().removeMembershipListener(membershipListenerReg);
            if (partitionListenerReg != null) {
                instance.getPartitionService().removeMigrationListener(partitionListenerReg);
            }
        }
    }
}
