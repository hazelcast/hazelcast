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

package com.hazelcast.jet.impl.actor.shuffling;

import com.hazelcast.spi.NodeEngine;
import com.hazelcast.jet.spi.data.DataWriter;
import com.hazelcast.core.PartitioningStrategy;
import com.hazelcast.jet.spi.strategy.ShufflingStrategy;
import com.hazelcast.jet.spi.dag.tap.SinkTapWriteStrategy;
import com.hazelcast.jet.spi.container.ContainerDescriptor;

public class ShufflingWriter extends ShufflingConsumer implements DataWriter {
    private final DataWriter dataWriter;

    public ShufflingWriter(DataWriter dataWriter, NodeEngine nodeEngine, ContainerDescriptor containerDescriptor) {
        super(dataWriter, nodeEngine, containerDescriptor);
        this.dataWriter = dataWriter;
    }

    @Override
    public SinkTapWriteStrategy getSinkTapWriteStrategy() {
        return dataWriter.getSinkTapWriteStrategy();
    }

    @Override
    public int getPartitionId() {
        return dataWriter.getPartitionId();
    }

    @Override
    public boolean isPartitioned() {
        return dataWriter.isPartitioned();
    }

    @Override
    public PartitioningStrategy getPartitionStrategy() {
        return dataWriter.getPartitionStrategy();
    }

    @Override
    public void open() {
        this.dataWriter.open();
    }

    @Override
    public void close() {
        this.dataWriter.close();
    }

    @Override
    public ShufflingStrategy getShufflingStrategy() {
        return dataWriter.getShufflingStrategy();
    }

    @Override
    public boolean isFlushed() {
        return dataWriter.isFlushed();
    }

    @Override
    public boolean isClosed() {
        return this.dataWriter.isClosed();
    }

    @Override
    public int flush() {
        return dataWriter.flush();
    }
}
