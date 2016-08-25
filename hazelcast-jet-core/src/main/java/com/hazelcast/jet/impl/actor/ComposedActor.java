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

package com.hazelcast.jet.impl.actor;

import com.hazelcast.core.PartitioningStrategy;
import com.hazelcast.jet.dag.Edge;
import com.hazelcast.jet.dag.Vertex;
import com.hazelcast.jet.data.io.ProducerInputStream;
import com.hazelcast.jet.impl.container.ContainerContextImpl;
import com.hazelcast.jet.impl.container.task.ContainerTask;
import com.hazelcast.jet.strategy.CalculationStrategy;
import com.hazelcast.jet.strategy.HashingStrategy;
import com.hazelcast.jet.strategy.RoutingStrategy;
import com.hazelcast.jet.strategy.MemberDistributionStrategy;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.util.List;

@SuppressFBWarnings("EI_EXPOSE_REP")
public class ComposedActor implements ObjectActor {
    private final Edge edge;
    private final Vertex vertex;
    private final ContainerTask task;
    private final ObjectActor[] consumers;
    private final RoutingStrategy routingStrategy;
    private final CalculationStrategy calculationStrategy;

    private int nextActorId;
    private int lastConsumedCount;

    public ComposedActor(
            ContainerTask task,
            List<ObjectActor> actors,
            Vertex vertex,
            Edge edge,
            ContainerContextImpl containerContext) {
        this.edge = edge;
        this.task = task;
        this.vertex = vertex;
        this.routingStrategy = edge.getRoutingStrategy();
        this.consumers = actors.toArray(new ObjectActor[actors.size()]);
        this.calculationStrategy = new CalculationStrategy(
                edge.getHashingStrategy(),
                edge.getPartitioningStrategy(),
                containerContext
        );
    }

    @Override
    public ContainerTask getSourceTask() {
        return task;
    }

    @Override
    public void registerCompletionHandler(ProducerCompletionHandler runnable) {
        for (ObjectActor actor : this.consumers) {
            actor.registerCompletionHandler(runnable);
        }
    }

    @Override
    public void handleProducerCompleted() {
        for (ObjectActor actor : this.consumers) {
            actor.handleProducerCompleted();
        }
    }

    @Override
    public int lastConsumedCount() {
        return this.lastConsumedCount;
    }

    @Override
    public int consumeObject(Object object) {
        if (this.routingStrategy == RoutingStrategy.ROUND_ROBIN) {
            this.consumers[nextActorId].consumeObject(object);
            next();
        } else if (this.routingStrategy == RoutingStrategy.BROADCAST) {
            for (ObjectActor actor : this.consumers) {
                actor.consumeObject(object);
            }
        } else if (this.routingStrategy == RoutingStrategy.PARTITIONED) {
            int objectPartitionId = calculatePartitionIndex(object);
            int idx = Math.abs(objectPartitionId) % this.consumers.length;
            this.consumers[idx].consumeObject(object);
        }

        return 1;
    }

    @Override
    public int consumeChunk(ProducerInputStream<Object> chunk) {
        if (this.routingStrategy == RoutingStrategy.ROUND_ROBIN) {
            this.consumers[nextActorId].consumeChunk(chunk);
            next();
        } else if (this.routingStrategy == RoutingStrategy.BROADCAST) {
            for (ObjectActor actor : this.consumers) {
                actor.consumeChunk(chunk);
            }
        } else if (this.routingStrategy == RoutingStrategy.PARTITIONED) {
            for (Object object : chunk) {
                consumeObject(object);
            }
        }

        this.lastConsumedCount = chunk.size();
        return chunk.size();
    }

    @SuppressWarnings("unchecked")
    private int calculatePartitionIndex(Object object) {
        return this.calculationStrategy.hash(object);
    }

    private void next() {
        if (this.nextActorId >= this.consumers.length - 1) {
            this.nextActorId = 0;
        } else {
            this.nextActorId++;
        }
    }

    @Override
    public int flush() {
        int flushed = 0;

        for (ObjectActor actor : this.consumers) {
            flushed += actor.flush();
        }

        return flushed;
    }

    @Override
    public boolean isFlushed() {
        boolean isFlushed = true;

        for (ObjectActor actor : this.consumers) {
            isFlushed &= actor.isFlushed();
        }

        return isFlushed;
    }

    public MemberDistributionStrategy getMemberDistributionStrategy() {
        return this.edge.getMemberDistributionStrategy();
    }

    @Override
    public PartitioningStrategy getPartitionStrategy() {
        return edge.getPartitioningStrategy();
    }

    @Override
    public HashingStrategy getHashingStrategy() {
        return edge.getHashingStrategy();
    }

    @Override
    public boolean isShuffled() {
        boolean isShuffled = true;

        for (ObjectActor actor : this.consumers) {
            isShuffled &= actor.isShuffled();
        }

        return isShuffled;
    }

    @Override
    public String getName() {
        return this.vertex.getName();
    }

    @Override
    public boolean isClosed() {
        boolean isClosed = true;

        for (ObjectActor actor : this.consumers) {
            isClosed &= actor.isClosed();
        }

        return isClosed;
    }

    @Override
    public void open() {
        for (ObjectActor actor : this.consumers) {
            actor.open();
        }
    }

    @Override
    public void close() {
        for (ObjectActor actor : this.consumers) {
            actor.close();
        }
    }

    @Override
    public Object[] produce() {
        throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public int lastProducedCount() {
        throw new UnsupportedOperationException("Not supported");
    }

    /**
         * @return parties of the composed actor
         */
    public ObjectActor[] getParties() {
        return this.consumers;
    }
}
