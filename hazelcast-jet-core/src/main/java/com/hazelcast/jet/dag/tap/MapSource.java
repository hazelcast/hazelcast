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
import com.hazelcast.jet.dag.Vertex;
import com.hazelcast.jet.data.DataReader;
import com.hazelcast.jet.data.tuple.JetTupleFactory;
import com.hazelcast.jet.impl.dag.tap.source.HazelcastMapPartitionReader;
import com.hazelcast.jet.impl.util.JetUtil;

import java.util.List;

public class MapSource implements SourceTap {

    private final String name;

    public MapSource(String name) {
        this.name = name;
    }

    public MapSource(IMap map) {
        this(map.getName());
    }

    @Override
    public DataReader[] getReaders(ContainerDescriptor containerDescriptor, Vertex vertex, JetTupleFactory tupleFactory) {
        List<Integer> localPartitions = JetUtil.getLocalPartitions(containerDescriptor.getNodeEngine());
        DataReader[] readers = new DataReader[localPartitions.size()];
        for (int i = 0; i < localPartitions.size(); i++) {
            int partitionId = localPartitions.get(i);
            readers[i] = getReader(containerDescriptor, tupleFactory, partitionId);
        }
        return readers;
    }

    protected DataReader getReader(ContainerDescriptor containerDescriptor,
                                   JetTupleFactory tupleFactory, int partitionId) {
        return new HazelcastMapPartitionReader(containerDescriptor, name, partitionId, tupleFactory);
    }

    @Override
    public String getName() {
        return name;
    }
}
