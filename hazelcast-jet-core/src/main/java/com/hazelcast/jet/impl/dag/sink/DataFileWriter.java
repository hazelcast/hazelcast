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

package com.hazelcast.jet.impl.dag.sink;

import com.hazelcast.core.PartitioningStrategy;
import com.hazelcast.jet.container.ContainerDescriptor;
import com.hazelcast.jet.data.io.ProducerInputStream;
import com.hazelcast.jet.io.tuple.Tuple2;
import com.hazelcast.partition.strategy.StringPartitioningStrategy;

public class DataFileWriter extends AbstractHazelcastWriter {
    private final FileOutputStream fileOutputStream;

    private boolean closed = true;

    public DataFileWriter(ContainerDescriptor containerDescriptor, int partitionID, FileOutputStream fileOutputStream) {
        super(containerDescriptor, partitionID);
        this.fileOutputStream = fileOutputStream;
        containerDescriptor.registerJobListener(jobContext -> closeFile());
    }

    @Override
    protected void processChunk(ProducerInputStream stream) {
        checkFileOpen();
        StringBuilder sb = new StringBuilder();
        for (Object o : stream) {
            Tuple2 t = (Tuple2) o;
            for (int i = 0; i < 2; i++) {
                sb.append(t.get(i).toString()).append(" ");
            }
            sb.append("\r\n");
        }
        if (sb.length() > 0) {
            fileOutputStream.write(sb.toString());
            fileOutputStream.flush();
        }
    }

    private void checkFileOpen() {
        if (closed) {
            fileOutputStream.open();
            closed = false;
        }
    }

    private void closeFile() {
        if (!closed) {
            fileOutputStream.close();
            closed = true;
        }
    }

    @Override
    public PartitioningStrategy getPartitionStrategy() {
        return StringPartitioningStrategy.INSTANCE;
    }

    @Override
    public boolean isPartitioned() {
        return false;
    }
}

