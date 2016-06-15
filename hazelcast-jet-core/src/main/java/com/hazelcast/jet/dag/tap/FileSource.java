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

import com.hazelcast.jet.container.ContainerDescriptor;
import com.hazelcast.jet.dag.Vertex;
import com.hazelcast.jet.data.DataReader;
import com.hazelcast.jet.data.tuple.JetTupleFactory;
import com.hazelcast.jet.impl.dag.tap.source.DataFileReader;
import com.hazelcast.jet.impl.util.JetUtil;

import java.io.File;

public class FileSource implements SourceTap {

    private final String name;

    public FileSource(String name) {
        this.name = name;
    }

    @Override
    public DataReader[] getReaders(ContainerDescriptor containerDescriptor, Vertex vertex, JetTupleFactory tupleFactory) {
        File file = new File(this.name);
        int chunkCount = vertex.getDescriptor().getTaskCount();
        long[] chunks = JetUtil.splitFile(file, chunkCount);
        DataReader[] readers = new DataReader[chunkCount];
        for (int i = 0; i < chunkCount; i++) {
            long start = chunks[i];

            if (start < 0) {
                break;
            }

            long end = i < chunkCount - 1 ? chunks[i + 1] : file.length();

            int partitionId = i % containerDescriptor.getNodeEngine().getPartitionService().getPartitionCount();
            readers[i] = new DataFileReader(containerDescriptor, vertex, partitionId, tupleFactory, name, start, end);
        }
        return readers;
    }

    @Override
    public String getName() {
        return name;
    }
}
