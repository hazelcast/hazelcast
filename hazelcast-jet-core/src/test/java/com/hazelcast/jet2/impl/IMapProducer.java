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

import com.hazelcast.core.Member;
import com.hazelcast.core.Partition;
import com.hazelcast.jet2.Outbox;
import com.hazelcast.jet2.Processor;
import com.hazelcast.jet2.ProcessorContext;
import com.hazelcast.jet2.ProcessorSupplier;
import com.hazelcast.map.impl.proxy.MapProxyImpl;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class IMapProducer extends AbstractProducer {

    private final static int BATCH_SIZE = 16;
    private final static int FETCH_SIZE = 128;

    private final MapProxyImpl map;
    private final List<Integer> partitions;

    private List<Iterator> iterators;

    private CircularCursor<Iterator> iteratorCursor;

    protected IMapProducer(MapProxyImpl map, List<Integer> partitions) {
        this.map = map;
        this.partitions = partitions;
        this.iterators = new ArrayList<>();
    }

    @Override
    public void init(@Nonnull Outbox outbox) {
        super.init(outbox);
        iterators = partitions.stream().map(p -> map.iterator(FETCH_SIZE, p, true))
                .collect(Collectors.toList());
        this.iteratorCursor = new CircularCursor<>(iterators);
    }

    @Override
    public boolean complete() {
        int count = 0;
        do {
            Iterator currIterator = iteratorCursor.value();
            if (!currIterator.hasNext()) {
                iteratorCursor.remove();
                continue;
            }
            emit(currIterator.next());
            if (count++ > BATCH_SIZE) {
                return false;
            }
        } while (iteratorCursor.advance());
        return true;
    }

    @Override
    public boolean isBlocking() {
        return true;
    }

    public static ProcessorSupplier supplier(String mapName) {
        return new Supplier(mapName);
    }

    private static class Supplier implements ProcessorSupplier {

        private final String name;
        private MapProxyImpl map;
        private int index = 0;
        private Map<Integer, List<Integer>> partitionGroups;

        public Supplier(String name) {
            this.name = name;
        }

        @Override
        public void init(ProcessorContext context) {
            // distribute local partitions
            Member localMember = context.getHazelcastInstance().getCluster().getLocalMember();
            Set<Partition> partitions = context.getHazelcastInstance().getPartitionService().getPartitions();
            partitionGroups = partitions.stream().filter(p -> p.getOwner().equals(localMember)).
                    map(Partition::getPartitionId)
                    .collect(Collectors.groupingBy(p -> p % context.parallelism()));
            this.map = (MapProxyImpl) context.getHazelcastInstance().getMap(name);
        }

        @Override
        public Processor get() {
            return new IMapProducer(map, partitionGroups.get(index++));
        }
    }
}
