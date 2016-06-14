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

package com.hazelcast.jet.impl.dag.tap.sink;

import com.hazelcast.core.PartitioningStrategy;
import com.hazelcast.jet.application.ApplicationListener;
import com.hazelcast.jet.container.ContainerDescriptor;
import com.hazelcast.jet.data.io.ProducerInputStream;
import com.hazelcast.jet.impl.application.ApplicationContext;
import com.hazelcast.jet.io.tuple.Tuple;
import com.hazelcast.partition.strategy.StringPartitioningStrategy;

public class DataFileWriter extends AbstractHazelcastWriter {
    private final FileOutputStream fileOutputStream;

    private boolean closed = true;

    public DataFileWriter(ContainerDescriptor containerDescriptor,
                          int partitionID,
                          FileOutputStream fileOutputStream) {
        super(containerDescriptor, partitionID);
        this.fileOutputStream = fileOutputStream;

        containerDescriptor.registerApplicationListener(new ApplicationListener() {
            @Override
            public void onApplicationExecuted(ApplicationContext applicationContext) {
                closeFile();
            }
        });
    }

    @Override
    protected void processChunk(ProducerInputStream stream) {
        checkFileOpen();

        StringBuilder sb = new StringBuilder();

        for (Object o : stream) {
            Tuple t = (Tuple) o;

            for (int i = 0; i < t.keySize(); i++) {
                sb.append(t.getKey(i).toString()).append(" ");
            }

            for (int i = 0; i < t.valueSize(); i++) {
                sb.append(t.getValue(i).toString()).append(" ");
            }

            sb.append("\r\n");
        }

        if (sb.length() > 0) {
            this.fileOutputStream.write(sb.toString());
            this.fileOutputStream.flush();
        }
    }

    private void checkFileOpen() {
        if (this.closed) {
            this.fileOutputStream.open();
            this.closed = false;
        }
    }

    private void closeFile() {
        if (!this.closed) {
            this.fileOutputStream.close();
            this.closed = true;
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

