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

package com.hazelcast.jet2;

import com.hazelcast.jet2.impl.IMapReader;
import com.hazelcast.map.impl.proxy.MapProxyImpl;
import com.hazelcast.nio.Address;

import java.io.Serializable;
import java.util.List;
import java.util.stream.Collectors;

/**
 *
 */
@FunctionalInterface
public interface ProcessorSupplier extends Serializable {

    /**
     * Javadoc pending
     */
    default void init(ProcessorSupplierContext context) {
    }

    /**
     * Javadoc pending
     */
    Processor get();


}


class IMapReaderMetaSupplier implements MetaProcessorSupplier {

    private final String mapName;
    private transient MetaProcessorSupplierContext context;

    public IMapReaderMetaSupplier(String mapName) {
        this.mapName = mapName;
    }

    @Override
    public void init(MetaProcessorSupplierContext context) {
        this.context = context;
    }

    @Override
    public ProcessorSupplier get(Address address) {
        List<Integer> ownedPartitions = context.getHazelcastInstance().getPartitionService().getPartitions()
                .stream().filter(f -> f.getOwner().getAddress().equals(address))
                .map(f -> f.getPartitionId())
                .collect(Collectors.toList());

        return new IMapReaderSupplier(mapName, ownedPartitions);
    }

    private static class IMapReaderSupplier implements ProcessorSupplier {

        private final String name;
        private final List<Integer> ownedPartitions;
        private int index;
        private transient MapProxyImpl map;
        private int localParallelism;

        public IMapReaderSupplier(String name, List<Integer> ownedPartitions) {
            this.name = name;
            this.ownedPartitions = ownedPartitions;
        }

        @Override
        public void init(ProcessorSupplierContext context) {
            index = 0;
            map = (MapProxyImpl) context.getHazelcastInstance().getMap(name);
            localParallelism = context.perNodeParallelism();
        }

        @Override
        public Processor get() {
            List<Integer> partitions = this.ownedPartitions.stream().filter(f -> f % localParallelism == index)
                    .collect(Collectors.toList());
            index++;
            return new IMapReader(map, partitions, 1024);
        }
    }
}