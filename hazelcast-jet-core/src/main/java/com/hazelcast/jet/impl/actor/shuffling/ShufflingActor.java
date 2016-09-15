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
import com.hazelcast.jet.impl.actor.ProducerCompletionHandler;
import com.hazelcast.jet.runtime.InputChunk;
import com.hazelcast.jet.impl.actor.Actor;
import com.hazelcast.jet.impl.actor.Consumer;
import com.hazelcast.jet.impl.runtime.task.VertexTask;
import com.hazelcast.jet.runtime.Producer;
import com.hazelcast.jet.strategy.HashingStrategy;
import com.hazelcast.jet.strategy.MemberDistributionStrategy;
import com.hazelcast.spi.NodeEngine;

public class ShufflingActor implements Actor {
    private final Producer producer;
    private final Actor baseActor;
    private final Consumer consumer;

    public ShufflingActor(Actor baseActor, NodeEngine nodeEngine) {
        this.producer = baseActor;
        this.baseActor = baseActor;
        this.consumer = new ShufflingConsumer(baseActor, nodeEngine);
    }


    // Simultaneous Producer and Consumer implementation

    @Override
    public String getName() {
        return producer.getName();
    }

    @Override
    public void open() {
        producer.open();
    }

    @Override
    public void close() {
        producer.close();
    }


    // Producer implementation

    @Override
    public Object[] produce() throws Exception {
        return this.producer.produce();
    }

    @Override
    public int lastProducedCount() {
        return this.producer.lastProducedCount();
    }

    @Override
    public void registerCompletionHandler(ProducerCompletionHandler runnable) {
        producer.registerCompletionHandler(runnable);
    }

    @Override
    public void handleProducerCompleted() {
        producer.handleProducerCompleted();
    }


    // Consumer implementation

    @Override
    public int consume(InputChunk<Object> chunk) {
        return this.consumer.consume(chunk);
    }

    @Override
    public int consume(Object object) {
        return this.consumer.consume(object);
    }

    @Override
    public boolean isShuffled() {
        return this.consumer.isShuffled();
    }

    @Override
    public int flush() {
        return this.consumer.flush();
    }

    @Override
    public boolean isFlushed() {
        return this.consumer.isFlushed();
    }

    @Override
    public int lastConsumedCount() {
        return consumer.lastConsumedCount();
    }

    @Override
    public MemberDistributionStrategy getMemberDistributionStrategy() {
        return this.consumer.getMemberDistributionStrategy();
    }

    @Override
    public PartitioningStrategy getPartitionStrategy() {
        return consumer.getPartitionStrategy();
    }

    @Override
    public HashingStrategy getHashingStrategy() {
        return consumer.getHashingStrategy();
    }


    // Actor implementation

    @Override
    public VertexTask getSourceTask() {
        return baseActor.getSourceTask();
    }



}
