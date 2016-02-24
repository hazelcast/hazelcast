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

package com.hazelcast.jet.impl.dag.tap.source;

import com.hazelcast.jet.impl.util.JetUtil;
import com.hazelcast.jet.spi.container.ContainerDescriptor;
import com.hazelcast.jet.spi.dag.Vertex;
import com.hazelcast.jet.spi.dag.tap.SourceTap;
import com.hazelcast.jet.spi.dag.tap.TapType;
import com.hazelcast.jet.spi.data.DataReader;
import com.hazelcast.jet.spi.data.tuple.TupleFactory;
import com.hazelcast.partition.IPartition;

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

    public DataReader[] getReaders(ContainerDescriptor containerDescriptor, Vertex vertex, TupleFactory tupleFactory) {
        List<DataReader> readers = new ArrayList<DataReader>();

        if (TapType.HAZELCAST_LIST == this.tapType) {
            int partitionId = HazelcastListPartitionReader.getPartitionId(containerDescriptor.getNodeEngine(), this.name);
            IPartition partition = containerDescriptor.getNodeEngine().getPartitionService().getPartition(partitionId);

            if (partition != null && partition.isLocal()) {
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
            for (IPartition partition : containerDescriptor.getNodeEngine().getPartitionService().getPartitions()) {
                if (partition.isLocal()) {
                    readers.add(HazelcastReaderFactory.getReader(
                            this.tapType,
                            this.name,
                            containerDescriptor,
                            partition.getPartitionId(),
                            tupleFactory,
                            vertex
                            )
                    );
                }
            }
        }

        return readers.toArray(new DataReader[readers.size()]);
    }
}
