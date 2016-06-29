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

import com.hazelcast.jet.container.ContainerDescriptor;
import com.hazelcast.jet.dag.Vertex;
import com.hazelcast.jet.data.tuple.JetTupleFactory;
import com.hazelcast.jet.impl.actor.ByReferenceDataTransferringStrategy;
import com.hazelcast.jet.io.tuple.Tuple;

import java.io.File;

public class DataFileReader extends AbstractHazelcastReader<Tuple<Integer, String>> {
    private final long end;
    private final long start;

    public DataFileReader(ContainerDescriptor containerDescriptor,
                          Vertex vertex,
                          int partitionId,
                          JetTupleFactory tupleFactory,
                          String name,
                          long start,
                          long end
    ) {
        super(containerDescriptor, name, partitionId, tupleFactory, ByReferenceDataTransferringStrategy.INSTANCE);
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
        this.iterator = null;
    }
}
