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

package com.hazelcast.jet2.impl;

import com.hazelcast.jet2.Outbox;
import com.hazelcast.jet2.Processor;
import com.hazelcast.jet2.ProcessorMetaSupplier;
import com.hazelcast.jet2.ProcessorSupplier;
import com.hazelcast.map.impl.proxy.MapProxyImpl;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.partition.IPartitionService;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;
import javax.annotation.Nonnull;

import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.mapping;
import static java.util.stream.Collectors.toList;

public class IMapReader extends AbstractProducer {

    public static final int DEFAULT_FETCH_SIZE = 16384;

    private final MapProxyImpl map;
    private final List<Integer> partitions;
    private final int fetchSize;

    private List<Iterator> iterators;

    private CircularCursor<Iterator> iteratorCursor;

    public IMapReader(MapProxyImpl map, List<Integer> partitions, int fetchSize) {
        this.map = map;
        this.partitions = partitions;
        this.fetchSize = fetchSize;
        this.iterators = new ArrayList<>();
    }

    @Override
    public void init(@Nonnull Outbox outbox) {
        super.init(outbox);
        iterators = partitions.stream().map(p -> map.iterator(fetchSize, p, true)).collect(toList());
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
    public boolean isBlocking() {
        return true;
    }

    public static ProcessorMetaSupplier supplier(String mapName) {
        return new MetaSupplier(mapName, DEFAULT_FETCH_SIZE);
    }

    public static ProcessorMetaSupplier supplier(String mapName, int fetchSize) {
        return new MetaSupplier(mapName, fetchSize);
    }

    private static class MetaSupplier implements ProcessorMetaSupplier {

        static final long serialVersionUID = 1L;

        private final String name;
        private final int fetchSize;

        private transient IPartitionService partitionService;

        MetaSupplier(String name, int fetchSize) {
            this.name = name;
            this.fetchSize = fetchSize;
        }

        @Override
        public void init(Context context) {
            partitionService = context.getPartitionServce();
        }

        @Override
        public ProcessorSupplier get(Address address) {
            List<Integer> ownedPartitions = partitionService.getMemberPartitionsMap().get(address);
            return new Supplier(name, ownedPartitions, fetchSize);
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
            Map<Integer, List<Integer>> processorToPartitions = IntStream.range(0, ownedPartitions.size()).boxed()
                    .map(i -> new AbstractMap.SimpleImmutableEntry<>(i, ownedPartitions.get(i)))
                    .collect(groupingBy(e -> e.getKey() % count,
                            mapping(e -> e.getValue(), toList())));
            IntStream.range(0, count)
                    .forEach(processor -> processorToPartitions.computeIfAbsent(processor, x -> emptyList()));
            return processorToPartitions
                    .values().stream()
                    .map(partitions -> !partitions.isEmpty()
                            ? new IMapReader(map, partitions, fetchSize)
                            : new AbstractProducer() { }
                    )
                    .collect(toList());
        }
    }
}
