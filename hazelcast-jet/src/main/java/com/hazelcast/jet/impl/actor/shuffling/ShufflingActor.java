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

import com.hazelcast.jet.spi.strategy.HashingStrategy;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.core.PartitioningStrategy;
import com.hazelcast.jet.api.actor.ObjectActor;
import com.hazelcast.jet.spi.strategy.ShufflingStrategy;
import com.hazelcast.jet.api.actor.ObjectConsumer;
import com.hazelcast.jet.api.container.ContainerTask;
import com.hazelcast.jet.api.container.ContainerContext;
import com.hazelcast.jet.api.data.io.ProducerInputStream;

public class ShufflingActor extends ShufflingProducer implements ObjectActor {
    private final ObjectActor baseActor;
    private final ObjectConsumer objectConsumer;

    public ShufflingActor(ObjectActor baseActor,
                          NodeEngine nodeEngine,
                          ContainerContext containerContext) {
        super(baseActor);
        this.baseActor = baseActor;
        this.objectConsumer = new ShufflingConsumer(baseActor, nodeEngine, containerContext);
    }

    @Override
    public ContainerTask getSourceTask() {
        return this.baseActor.getSourceTask();
    }

    @Override
    public int consumeChunk(ProducerInputStream<Object> chunk) throws Exception {
        return this.objectConsumer.consumeChunk(chunk);
    }

    @Override
    public int consumeObject(Object object) throws Exception {
        return this.objectConsumer.consumeObject(object);
    }

    @Override
    public boolean isShuffled() {
        return this.objectConsumer.isShuffled();
    }

    @Override
    public String getName() {
        return baseActor.getName();
    }

    @Override
    public int flush() {
        return this.objectConsumer.flush();
    }

    @Override
    public boolean isFlushed() {
        return this.objectConsumer.isFlushed();
    }

    @Override
    public int lastConsumedCount() {
        return objectConsumer.lastConsumedCount();
    }

    @Override
    public ShufflingStrategy getShufflingStrategy() {
        return this.objectConsumer.getShufflingStrategy();
    }

    @Override
    public PartitioningStrategy getPartitionStrategy() {
        return objectConsumer.getPartitionStrategy();
    }

    @Override
    public HashingStrategy getHashingStrategy() {
        return objectConsumer.getHashingStrategy();
    }

    @Override
    public boolean consume(ProducerInputStream<Object> inputStream) throws Exception {
        return this.objectConsumer.consume(inputStream);
    }
}
