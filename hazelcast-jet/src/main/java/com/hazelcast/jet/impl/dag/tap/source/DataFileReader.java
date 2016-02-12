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

import com.hazelcast.jet.impl.actor.ByReferenceDataTransferringStrategy;
import com.hazelcast.jet.impl.util.JetUtil;
import com.hazelcast.jet.spi.container.ContainerDescriptor;
import com.hazelcast.jet.spi.dag.Vertex;
import com.hazelcast.jet.spi.data.tuple.TupleFactory;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;

public class DataFileReader extends AbstractHazelcastReader<String> {
    private final long end;
    private final long start;
    private FileReader fileReader;

    public DataFileReader(ContainerDescriptor containerDescriptor,
                          Vertex vertex,
                          int partitionId,
                          TupleFactory tupleFactory,
                          String name,
                          long start,
                          long end
    ) {
        super(containerDescriptor, name, partitionId, tupleFactory, vertex, ByReferenceDataTransferringStrategy.INSTANCE);
        this.end = end;
        this.start = start;
    }

    @Override
    public boolean readFromPartitionThread() {
        return false;
    }

    @Override
    protected void onOpen() {
        File file = new File(getName());
        this.iterator = new FileIterator(file, this.start, this.end);
        this.position = ((FileIterator) this.iterator).getLineNumber();
    }

    @Override
    protected void onClose() {
        if (this.fileReader != null) {
            try {
                this.iterator = null;
                this.fileReader.close();
                this.fileReader = null;
            } catch (IOException e) {
                throw JetUtil.reThrow(e);
            }
        }
    }
}
