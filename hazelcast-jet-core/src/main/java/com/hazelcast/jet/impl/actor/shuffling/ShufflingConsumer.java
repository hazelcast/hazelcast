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


import com.hazelcast.core.PartitioningStrategy;
import com.hazelcast.jet.impl.actor.ObjectConsumer;
import com.hazelcast.jet.data.io.ProducerInputStream;
import com.hazelcast.jet.container.ContainerDescriptor;
import com.hazelcast.jet.strategy.HashingStrategy;
import com.hazelcast.jet.strategy.ShufflingStrategy;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.impl.NodeEngineImpl;

public class ShufflingConsumer implements ObjectConsumer {
    private final ObjectConsumer baseConsumer;
    private final NodeEngineImpl nodeEngine;
    private final ContainerDescriptor containerDescriptor;

    public ShufflingConsumer(ObjectConsumer baseConsumer,
                             NodeEngine nodeEngine,
                             ContainerDescriptor containerDescriptor) {
        this.baseConsumer = baseConsumer;
        this.containerDescriptor = containerDescriptor;
        this.nodeEngine = (NodeEngineImpl) nodeEngine;
    }

    @Override
    public int consumeChunk(ProducerInputStream<Object> chunk) throws Exception {
        return this.baseConsumer.consumeChunk(chunk);
    }

    @Override
    public int consumeObject(Object object) throws Exception {
        return this.baseConsumer.consumeObject(object);
    }

    @Override
    public int flush() {
        return this.baseConsumer.flush();
    }

    @Override
    public boolean isFlushed() {
        return this.baseConsumer.isFlushed();
    }

    @Override
    public boolean isShuffled() {
        return true;
    }

    @Override
    public void open() {
        this.baseConsumer.open();
    }

    @Override
    public void close() {
        this.baseConsumer.close();
    }

    @Override
    public int lastConsumedCount() {
        return this.baseConsumer.lastConsumedCount();
    }

    @Override
    public ShufflingStrategy getShufflingStrategy() {
        return this.baseConsumer.getShufflingStrategy();
    }

    @Override
    public PartitioningStrategy getPartitionStrategy() {
        return this.baseConsumer.getPartitionStrategy();
    }

    @Override
    public HashingStrategy getHashingStrategy() {
        return this.baseConsumer.getHashingStrategy();
    }

    @Override
    public boolean consume(ProducerInputStream chunk) throws Exception {
        return consumeChunk(chunk) > 0;
    }

    public NodeEngineImpl getNodeEngine() {
        return nodeEngine;
    }
}
