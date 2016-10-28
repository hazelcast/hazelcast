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

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet2.MetaProcessorSupplier;
import com.hazelcast.jet2.MetaProcessorSupplierContext;
import com.hazelcast.jet2.Outbox;
import com.hazelcast.jet2.Processor;
import com.hazelcast.jet2.ProcessorSupplier;
import com.hazelcast.jet2.ProcessorSupplierContext;
import com.hazelcast.map.impl.proxy.MapProxyImpl;
import com.hazelcast.nio.Address;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

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
        iterators = partitions.stream().map(p -> map.iterator(fetchSize, p, true))
                .collect(Collectors.toList());
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

    public static MetaProcessorSupplier supplier(String mapName) {
        return new MetaSupplier(mapName, DEFAULT_FETCH_SIZE);
    }

    public static MetaProcessorSupplier supplier(String mapName, int fetchSize) {
        return new MetaSupplier(mapName, fetchSize);
    }

    private static class MetaSupplier implements MetaProcessorSupplier {

        static final long serialVersionUID = 1L;

        private final String name;
        private final int fetchSize;

        private transient HazelcastInstance hazelcastInstance;

        public MetaSupplier(String name, int fetchSize) {
            this.name = name;
            this.fetchSize = fetchSize;
        }

        @Override
        public void init(MetaProcessorSupplierContext context) {
            hazelcastInstance = context.getHazelcastInstance();
        }

        @Override
        public ProcessorSupplier get(Address address) {
            List<Integer> ownedPartitions = hazelcastInstance.getPartitionService().getPartitions()
                    .stream().filter(f -> f.getOwner().getAddress().equals(address))
                    .map(f -> f.getPartitionId())
                    .collect(Collectors.toList());
            return new Supplier(name, ownedPartitions, fetchSize);
        }
    }

    private static class Supplier implements ProcessorSupplier {

        static final long serialVersionUID = 1L;

        private final String name;
        private final List<Integer> ownedPartitions;
        private final int fetchSize;
        private int index;
        private int perNodeParallelism;

        private transient MapProxyImpl map;

        public Supplier(String name, List<Integer> ownedPartitions, int fetchSize) {
            this.name = name;
            this.ownedPartitions = ownedPartitions;
            this.fetchSize = fetchSize;
        }

        @Override
        public void init(ProcessorSupplierContext context) {
            index = 0;
            map = (MapProxyImpl) context.getHazelcastInstance().getMap(name);
            perNodeParallelism = context.perNodeParallelism();
        }

        @Override
        public Processor get() {
            List<Integer> partitions = this.ownedPartitions.stream().filter(f -> f % perNodeParallelism == index)
                    .collect(Collectors.toList());
            index++;
            return new IMapReader(map, partitions, fetchSize);
        }
    }
}
