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

package com.hazelcast.jet.impl.connector;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.impl.HazelcastClientProxy;
import com.hazelcast.client.proxy.ClientMapProxy;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.Member;
import com.hazelcast.core.Partition;
import com.hazelcast.jet.Outbox;
import com.hazelcast.jet.Processor;
import com.hazelcast.jet.ProcessorMetaSupplier;
import com.hazelcast.jet.ProcessorSupplier;
import com.hazelcast.jet.impl.util.CircularCursor;
import com.hazelcast.map.impl.proxy.MapProxyImpl;
import com.hazelcast.nio.Address;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.IntStream;

import static java.util.AbstractMap.SimpleImmutableEntry;
import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.mapping;
import static java.util.stream.Collectors.toList;
import static java.util.stream.IntStream.range;

public final class IMapReader extends AbstractProducer {

    private static final int DEFAULT_FETCH_SIZE = 16384;

    private final Function<Integer, Iterator<Map.Entry>> supplier;
    private final List<Integer> partitions;

    private List<Iterator> iterators;
    private CircularCursor<Iterator> iteratorCursor;

    private IMapReader(Function<Integer, Iterator<Map.Entry>> supplier, List<Integer> partitions) {
        this.supplier = supplier;
        this.partitions = partitions;
        this.iterators = new ArrayList<>();
    }

    @Override
    public void init(@Nonnull Outbox outbox) {
        super.init(outbox);
        iterators = partitions.stream().map(supplier::apply).collect(toList());
        this.iteratorCursor = new CircularCursor<>(iterators);
    }

    @Override
    public boolean complete() {
        do {
            Iterator<Map.Entry> currIterator = iteratorCursor.value();
            if (!currIterator.hasNext()) {
                iteratorCursor.remove();
                continue;
            }
            emit(currIterator.next());
            if (getOutbox().isHighWater()) {
                return false;
            }
        } while (iteratorCursor.advance());
        return true;
    }

    @Override
    public boolean isCooperative() {
        return false;
    }

    public static ProcessorMetaSupplier supplier(String mapName) {
        return new MetaSupplier(mapName, DEFAULT_FETCH_SIZE);
    }

    public static ProcessorMetaSupplier supplier(String mapName, int fetchSize) {
        return new MetaSupplier(mapName, fetchSize);
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
        private final SerializableClientConfig serializableClientConfig;
        private transient Map<Address, List<Integer>> memberToPartitions;


        RemoteClusterMetaSupplier(String name, int fetchSize, ClientConfig clientConfig) {
            this.name = name;
            this.fetchSize = fetchSize;
            this.serializableClientConfig = new SerializableClientConfig(clientConfig);
        }

        @Override
        public void init(Context context) {
            List<Member> members = new ArrayList<>(context.getHazelcastInstance().getCluster().getMembers());
            int memberSize = members.size();
            HazelcastInstance client = HazelcastClient.newHazelcastClient(serializableClientConfig.asClientConfig());
            try {
                HazelcastClientProxy clientProxy = (HazelcastClientProxy) client;
                int partitionCount = clientProxy.client.getClientPartitionService().getPartitionCount();
                memberToPartitions = IntStream.range(0, partitionCount).boxed().collect(
                        groupingBy(partition -> members.get(partition % memberSize).getAddress())
                );
            } finally {
                client.shutdown();
            }
        }

        @Override
        public ProcessorSupplier get(Address address) {
            List<Integer> ownedPartitions = memberToPartitions.get(address);
            return new RemoteClusterProcessorSupplier(name, fetchSize, ownedPartitions, serializableClientConfig);
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
        public void init(Context context) {
            client = HazelcastClient.newHazelcastClient(serializableClientConfig.asClientConfig());
            map = (ClientMapProxy) client.getMap(mapName);
        }

        @Override
        public void complete(Throwable error) {
            client.shutdown();
        }

        @Override
        public List<Processor> get(int count) {
            return getProcessors(count, ownedPartitions, partitionId -> map.iterator(fetchSize, partitionId, true));
        }
    }

    private static class MetaSupplier implements ProcessorMetaSupplier {

        static final long serialVersionUID = 1L;

        private final String name;
        private final int fetchSize;

        private transient Map<Address, List<Integer>> membersToPartitions;

        MetaSupplier(String name, int fetchSize) {
            this.name = name;
            this.fetchSize = fetchSize;
        }

        @Override
        public void init(Context context) {
            membersToPartitions = context
                    .getHazelcastInstance().getPartitionService()
                    .getPartitions().stream()
                    .collect(groupingBy(p -> p.getOwner().getAddress(), mapping(Partition::getPartitionId, toList())));
        }

        @Override
        public ProcessorSupplier get(Address address) {
            return new Supplier(name, membersToPartitions.get(address), fetchSize);
        }
    }

    private static class Supplier implements ProcessorSupplier {

        static final long serialVersionUID = 1L;

        private final String mapName;
        private final List<Integer> ownedPartitions;
        private final int fetchSize;

        private transient MapProxyImpl map;

        Supplier(String mapName, List<Integer> ownedPartitions, int fetchSize) {
            this.mapName = mapName;
            this.ownedPartitions = ownedPartitions;
            this.fetchSize = fetchSize;
        }

        @Override
        public void init(Context context) {
            map = (MapProxyImpl) context.getHazelcastInstance().getMap(mapName);
        }

        @Override
        public List<Processor> get(int count) {
            return getProcessors(count, ownedPartitions, partitionId -> map.iterator(fetchSize, partitionId, true));
        }

    }

    static List<Processor> getProcessors(int count, List<Integer> ownedPartitions,
                                         Function<Integer, Iterator<Map.Entry>> supplier) {
        Map<Integer, List<Integer>> processorToPartitions = range(0, ownedPartitions.size()).boxed()
                .map(i -> new SimpleImmutableEntry<>(i, ownedPartitions.get(i)))
                .collect(groupingBy(e -> e.getKey() % count, mapping(Map.Entry::getValue, toList())));
        range(0, count).forEach(processor -> processorToPartitions.computeIfAbsent(processor, x -> emptyList()));
        return processorToPartitions
                .values().stream()
                .map(partitions -> !partitions.isEmpty()
                                ? new IMapReader(supplier, partitions)
                                : new AbstractProducer() {
                        }
                )
                .collect(toList());
    }
}
