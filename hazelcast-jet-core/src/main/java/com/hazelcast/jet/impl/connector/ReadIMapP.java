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

package com.hazelcast.jet.impl.connector;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.impl.HazelcastClientProxy;
import com.hazelcast.client.proxy.ClientMapProxy;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.Partition;
import com.hazelcast.jet.AbstractProcessor;
import com.hazelcast.jet.Processor;
import com.hazelcast.jet.ProcessorMetaSupplier;
import com.hazelcast.jet.ProcessorSupplier;
import com.hazelcast.jet.Processors.NoopP;
import com.hazelcast.jet.Traverser;
import com.hazelcast.jet.impl.util.CircularListCursor;
import com.hazelcast.map.impl.proxy.MapProxyImpl;
import com.hazelcast.nio.Address;

import javax.annotation.Nonnull;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Function;
import java.util.stream.IntStream;

import static com.hazelcast.client.HazelcastClient.newHazelcastClient;
import static com.hazelcast.jet.Util.entry;
import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.mapping;
import static java.util.stream.Collectors.toList;
import static java.util.stream.IntStream.range;

public final class ReadIMapP extends AbstractProcessor {

    private static final int DEFAULT_FETCH_SIZE = 16384;

    private final Traverser<Entry> outputTraverser;

    ReadIMapP(Function<Integer, Iterator<Entry>> partitionToIterator, List<Integer> partitions) {
        final CircularListCursor<Iterator> iteratorCursor = new CircularListCursor<>(
                partitions.stream().map(partitionToIterator).collect(toList())
        );
        this.outputTraverser = () -> {
            do {
                final Iterator<Entry> currIterator = iteratorCursor.value();
                if (currIterator.hasNext()) {
                    iteratorCursor.advance();
                    return currIterator.next();
                }
                iteratorCursor.remove();
            } while (iteratorCursor.advance());
            return null;
        };
    }

    @Override
    public boolean complete() {
        return emitCooperatively(outputTraverser);
    }

    @Override
    public boolean isCooperative() {
        return false;
    }

    public static ProcessorMetaSupplier supplier(String mapName) {
        return new LocalClusterMetaSupplier(mapName, DEFAULT_FETCH_SIZE);
    }

    public static ProcessorMetaSupplier supplier(String mapName, int fetchSize) {
        return new LocalClusterMetaSupplier(mapName, fetchSize);
    }

    public static ProcessorMetaSupplier supplier(String mapName, int fetchSize, ClientConfig clientConfig) {
        return new RemoteClusterMetaSupplier(mapName, fetchSize, clientConfig);
    }

    public static ProcessorMetaSupplier supplier(String mapName, ClientConfig clientConfig) {
        return new RemoteClusterMetaSupplier(mapName, DEFAULT_FETCH_SIZE, clientConfig);
    }

    private static class RemoteClusterMetaSupplier implements ProcessorMetaSupplier {

        static final long serialVersionUID = 1L;

        private final String name;
        private final int fetchSize;
        private final SerializableClientConfig serializableConfig;

        private transient int remotePartitionCount;

        RemoteClusterMetaSupplier(String name, int fetchSize, ClientConfig clientConfig) {
            this.name = name;
            this.fetchSize = fetchSize;
            this.serializableConfig = new SerializableClientConfig(clientConfig);
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

            return address -> new RemoteClusterProcessorSupplier(name, fetchSize, membersToPartitions.get(address),
                    serializableConfig);
        }
    }

    private static class RemoteClusterProcessorSupplier implements ProcessorSupplier {

        static final long serialVersionUID = 1L;

        private final String mapName;
        private final int fetchSize;
        private List<Integer> ownedPartitions;
        private SerializableClientConfig serializableClientConfig;

        private transient HazelcastInstance client;
        private transient ClientMapProxy map;

        RemoteClusterProcessorSupplier(String mapName, int fetchSize, List<Integer> ownedPartitions,
                                       SerializableClientConfig serializableClientConfig) {
            this.mapName = mapName;
            this.fetchSize = fetchSize;
            this.ownedPartitions = ownedPartitions;
            this.serializableClientConfig = serializableClientConfig;
        }

        @Override
        public void init(@Nonnull Context context) {
            client = newHazelcastClient(serializableClientConfig.asClientConfig());
            map = (ClientMapProxy) client.getMap(mapName);
        }

        @Override
        public void complete(Throwable error) {
            client.shutdown();
        }

        @Override @Nonnull
        public List<Processor> get(int count) {
            return getProcessors(count, ownedPartitions, partitionId -> map.iterator(fetchSize, partitionId, true));
        }
    }

    private static class LocalClusterMetaSupplier implements ProcessorMetaSupplier {

        static final long serialVersionUID = 1L;

        private final String name;
        private final int fetchSize;

        private transient Map<Address, List<Integer>> addrToPartitions;

        LocalClusterMetaSupplier(String name, int fetchSize) {
            this.name = name;
            this.fetchSize = fetchSize;
        }

        @Override
        public void init(@Nonnull Context context) {
            addrToPartitions = context.jetInstance().getHazelcastInstance().getPartitionService().getPartitions()
                                      .stream()
                                      .collect(groupingBy(p -> p.getOwner().getAddress(),
                                              mapping(Partition::getPartitionId, toList())));
        }

        @Override @Nonnull
        public Function<Address, ProcessorSupplier> get(@Nonnull List<Address> addresseses) {
           return address -> new LocalClusterProcessorSupplier(name, addrToPartitions.get(address), fetchSize);
        }
    }

    private static class LocalClusterProcessorSupplier implements ProcessorSupplier {

        static final long serialVersionUID = 1L;

        private final String mapName;
        private final List<Integer> ownedPartitions;
        private final int fetchSize;

        private transient MapProxyImpl map;

        LocalClusterProcessorSupplier(String mapName, List<Integer> ownedPartitions, int fetchSize) {
            this.mapName = mapName;
            this.ownedPartitions = ownedPartitions != null ? ownedPartitions : Collections.emptyList();
            this.fetchSize = fetchSize;
        }

        @Override
        public void init(@Nonnull Context context) {
            map = (MapProxyImpl) context.jetInstance().getHazelcastInstance().getMap(mapName);
        }

        @Override @Nonnull
        public List<Processor> get(int count) {
            return getProcessors(count, ownedPartitions,
                    partitionId -> map.iterator(fetchSize, partitionId, true));
        }

    }

    static List<Processor> getProcessors(int count, List<Integer> ownedPartitions,
                                         Function<Integer, Iterator<Entry>> partitionToIterator) {
        Map<Integer, List<Integer>> processorToPartitions =
                range(0, ownedPartitions.size())
                        .mapToObj(i -> entry(i, ownedPartitions.get(i)))
                        .collect(groupingBy(e -> e.getKey() % count, mapping(Entry::getValue, toList())));
        range(0, count).forEach(processor -> processorToPartitions.computeIfAbsent(processor, x -> emptyList()));
        return processorToPartitions
                .values().stream()
                .map(partitions -> !partitions.isEmpty()
                        ? new ReadIMapP(partitionToIterator, partitions)
                        : new NoopP()
                )
                .collect(toList());
    }
}
