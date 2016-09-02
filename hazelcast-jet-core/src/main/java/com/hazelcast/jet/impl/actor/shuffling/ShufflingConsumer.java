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
import com.hazelcast.jet.runtime.InputChunk;
import com.hazelcast.jet.impl.actor.Consumer;
import com.hazelcast.jet.strategy.HashingStrategy;
import com.hazelcast.jet.strategy.MemberDistributionStrategy;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.spi.impl.NodeEngineImpl;

public class ShufflingConsumer implements Consumer {
    private final Consumer baseConsumer;
    private final NodeEngineImpl nodeEngine;

    public ShufflingConsumer(Consumer baseConsumer,
                             NodeEngine nodeEngine) {
        this.baseConsumer = baseConsumer;
        this.nodeEngine = (NodeEngineImpl) nodeEngine;
    }

    @Override
    public int consume(InputChunk<Object> chunk) {
        return this.baseConsumer.consume(chunk);
    }

    @Override
    public int consume(Object object) {
        return this.baseConsumer.consume(object);
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

    public MemberDistributionStrategy getMemberDistributionStrategy() {
        return this.baseConsumer.getMemberDistributionStrategy();
    }

    @Override
    public PartitioningStrategy getPartitionStrategy() {
        return this.baseConsumer.getPartitionStrategy();
    }

    @Override
    public HashingStrategy getHashingStrategy() {
        return this.baseConsumer.getHashingStrategy();
    }

    public NodeEngineImpl getNodeEngine() {
        return nodeEngine;
    }
}
