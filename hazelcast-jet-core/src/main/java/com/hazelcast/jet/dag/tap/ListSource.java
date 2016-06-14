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

import com.hazelcast.core.IList;
import com.hazelcast.jet.container.ContainerDescriptor;
import com.hazelcast.jet.dag.Vertex;
import com.hazelcast.jet.data.DataReader;
import com.hazelcast.jet.data.tuple.JetTupleFactory;
import com.hazelcast.jet.impl.dag.tap.source.HazelcastListPartitionReader;
import com.hazelcast.jet.impl.util.JetUtil;

public class ListSource implements SourceTap {

    private final String name;

    public ListSource(String name) {
        this.name = name;
    }

    public ListSource(IList list) {
        this(list.getName());
    }

    @Override
    public DataReader[] getReaders(ContainerDescriptor containerDescriptor, Vertex vertex, JetTupleFactory tupleFactory) {
        int partitionId = HazelcastListPartitionReader.getPartitionId(containerDescriptor.getNodeEngine(), this.name);
        if (JetUtil.isPartitionLocal(containerDescriptor.getNodeEngine(), partitionId)) {
            HazelcastListPartitionReader reader
                    = new HazelcastListPartitionReader(containerDescriptor, name, tupleFactory);

            return new DataReader[]{reader};
        }
        return new DataReader[0];
    }

    @Override
    public String getName() {
        return this.name;
    }
}
