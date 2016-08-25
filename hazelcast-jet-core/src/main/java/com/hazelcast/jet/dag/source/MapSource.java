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

package com.hazelcast.jet.dag.source;

import com.hazelcast.core.IMap;
import com.hazelcast.jet.container.ContainerContext;
import com.hazelcast.jet.dag.Vertex;
import com.hazelcast.jet.impl.actor.ObjectProducer;
import com.hazelcast.jet.impl.dag.source.MapPartitionReader;
import com.hazelcast.jet.impl.util.JetUtil;

import java.util.List;

/**
 * A source which uses a Hazelcast {@code IMap} as the input.
 */
public class MapSource implements Source {

    private final String name;

    /**
     * Constructs a source with the given map name.
     *
     * @param name of the map to use as the input
     */
    public MapSource(String name) {
        this.name = name;
    }

    /**
     * Constructs a source with the given map.
     *
     * @param map the map instance to be used as the input
     */
    public MapSource(IMap map) {
        this(map.getName());
    }

    @Override
    public ObjectProducer[] getReaders(ContainerContext containerContext, Vertex vertex) {
        List<Integer> localPartitions = JetUtil.getLocalPartitions(containerContext.getNodeEngine());
        ObjectProducer[] readers = new ObjectProducer[localPartitions.size()];
        for (int i = 0; i < localPartitions.size(); i++) {
            int partitionId = localPartitions.get(i);
            readers[i] = getReader(containerContext, partitionId);
        }
        return readers;
    }

    protected ObjectProducer getReader(ContainerContext containerContext, int partitionId) {
        return new MapPartitionReader(containerContext, name, partitionId);
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public String toString() {
        return "MapSource{"
                + "name='" + name + '\''
                + '}';
    }
}
