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

package com.hazelcast.jet.dag.tap;

import com.hazelcast.core.IMap;
import com.hazelcast.jet.container.ContainerDescriptor;
import com.hazelcast.jet.data.DataWriter;
import com.hazelcast.jet.impl.actor.shuffling.ShufflingWriter;
import com.hazelcast.jet.impl.dag.tap.sink.HazelcastMapPartitionWriter;
import com.hazelcast.jet.impl.util.JetUtil;
import com.hazelcast.spi.NodeEngine;

import java.util.List;

/**
 * A sink which uses a Hazelcast {@code IMap} as output.
 */
public class MapSink implements SinkTap {
    private final String name;

    /**
     * Constructs a sink with the given map name.
     *
     * @param name of the map to use as the output
     */
    public MapSink(String name) {
        this.name = name;
    }

    /**
     * Constructs a sink with the given list instance.
     *
     * @param map the map instance to be used as the output
     */
    public MapSink(IMap map) {
        this(map.getName());
    }

    @Override
    public DataWriter[] getWriters(NodeEngine nodeEngine, ContainerDescriptor containerDescriptor) {
        List<Integer> localPartitions = JetUtil.getLocalPartitions(nodeEngine);
        DataWriter[] writers = new DataWriter[localPartitions.size()];
        for (int i = 0; i < localPartitions.size(); i++) {
            int partitionId = localPartitions.get(i);
            writers[i] = new ShufflingWriter(
                    getPartitionWriter(containerDescriptor, partitionId),
                    nodeEngine,
                    containerDescriptor);
        }
        return writers;
    }

    protected DataWriter getPartitionWriter(ContainerDescriptor containerDescriptor, int partitionId) {
        return new HazelcastMapPartitionWriter(containerDescriptor, partitionId, name);
    }

    @Override
    public boolean isPartitioned() {
        return true;
    }

    @Override
    public String getName() {
        return name;
    }
}
