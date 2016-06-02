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

package com.hazelcast.jet.api.dag.tap;

import com.hazelcast.jet.internal.impl.dag.tap.source.HazelcastListPartitionReader;
import com.hazelcast.jet.internal.impl.dag.tap.source.HazelcastReaderFactory;
import com.hazelcast.jet.internal.impl.util.JetUtil;
import com.hazelcast.jet.api.container.ContainerDescriptor;
import com.hazelcast.jet.api.dag.Vertex;
import com.hazelcast.jet.api.data.DataReader;
import com.hazelcast.jet.api.data.tuple.JetTupleFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

public class HazelcastSourceTap extends SourceTap {
    private final String name;
    private final TapType tapType;

    public HazelcastSourceTap(String name, TapType tapType) {
        this.name = name;
        this.tapType = tapType;
    }

    public String getName() {
        return this.name;
    }

    @Override
    public TapType getType() {
        return this.tapType;
    }

    public DataReader[] getReaders(ContainerDescriptor containerDescriptor, Vertex vertex, JetTupleFactory tupleFactory) {
        List<DataReader> readers = new ArrayList<DataReader>();

        if (TapType.HAZELCAST_LIST == this.tapType) {
            int partitionId = HazelcastListPartitionReader.getPartitionId(containerDescriptor.getNodeEngine(), this.name);
            if (JetUtil.isPartitionLocal(containerDescriptor.getNodeEngine(), partitionId)) {
                readers.add(HazelcastReaderFactory.getReader(
                                this.tapType, this.name, containerDescriptor, partitionId, tupleFactory, vertex
                        )
                );
            }
        } else if (TapType.FILE == this.tapType) {
            File file = new File(this.name);
            int chunkCount = vertex.getDescriptor().getTaskCount();
            long[] chunks = JetUtil.splitFile(file, chunkCount);

            for (int i = 0; i < chunkCount; i++) {
                long start = chunks[i];

                if (start < 0) {
                    break;
                }

                long end = i < chunkCount - 1 ? chunks[i + 1] : file.length();

                readers.add(HazelcastReaderFactory.getReader(
                        this.tapType,
                        this.name,
                        containerDescriptor,
                        i % containerDescriptor.getNodeEngine().getPartitionService().getPartitionCount(),
                        start,
                        end,
                        tupleFactory,
                        vertex
                ));
            }
        } else {
            List<Integer> localPartitions = JetUtil.getLocalPartitions(containerDescriptor.getNodeEngine());
            for (int partitionId : localPartitions) {
                readers.add(HazelcastReaderFactory.getReader(
                        this.tapType,
                        this.name,
                        containerDescriptor,
                        partitionId,
                        tupleFactory,
                        vertex
                ));
            }
        }

        return readers.toArray(new DataReader[readers.size()]);
    }
}
