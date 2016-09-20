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

package com.hazelcast.jet.impl.ringbuffer;

import com.hazelcast.core.PartitioningStrategy;
import com.hazelcast.jet.Edge;
import com.hazelcast.jet.impl.job.JobContext;
import com.hazelcast.jet.runtime.Consumer;
import com.hazelcast.jet.runtime.InputChunk;
import com.hazelcast.jet.strategy.CalculationStrategy;
import com.hazelcast.jet.strategy.HashingStrategy;
import com.hazelcast.jet.strategy.MemberDistributionStrategy;
import com.hazelcast.jet.strategy.RoutingStrategy;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.util.List;

@SuppressFBWarnings("EI_EXPOSE_REP")
public class CompositeRingbuffer implements Consumer {
    private final Ringbuffer[] ringbuffers;
    private final RoutingStrategy routingStrategy;
    private final CalculationStrategy calculationStrategy;
    private final MemberDistributionStrategy memberDistributionStrategy;

    private int nextConsumerId;

    public CompositeRingbuffer(
            JobContext jobContext,
            List<Ringbuffer> ringbuffers,
            Edge edge) {
        this.routingStrategy = edge.getRoutingStrategy();
        this.ringbuffers = ringbuffers.toArray(new Ringbuffer[ringbuffers.size()]);
        this.memberDistributionStrategy = edge.getMemberDistributionStrategy();
        this.calculationStrategy = new CalculationStrategy(
                edge.getHashingStrategy(),
                edge.getPartitioningStrategy(),
                jobContext
        );
    }

    @Override
    public boolean consume(Object object) {
        if (routingStrategy == RoutingStrategy.ROUND_ROBIN) {
            ringbuffers[nextConsumerId].consume(object);
            next();
        } else if (routingStrategy == RoutingStrategy.BROADCAST) {
            for (Consumer consumer : ringbuffers) {
                consumer.consume(object);
            }
        } else if (routingStrategy == RoutingStrategy.PARTITIONED) {
            int objectPartitionId = calculatePartitionIndex(object);
            int idx = Math.abs(objectPartitionId) % ringbuffers.length;
            ringbuffers[idx].consume(object);
        }

        return true;
    }

    @Override
    public int consume(InputChunk<Object> chunk) {
        if (routingStrategy == RoutingStrategy.ROUND_ROBIN) {
            ringbuffers[nextConsumerId].consume(chunk);
            next();
        } else if (routingStrategy == RoutingStrategy.BROADCAST) {
            for (Consumer consumer : ringbuffers) {
                consumer.consume(chunk);
            }
        } else if (routingStrategy == RoutingStrategy.PARTITIONED) {
            for (Object object : chunk) {
                consume(object);
            }
        }

        return chunk.size();
    }

    @SuppressWarnings("unchecked")
    private int calculatePartitionIndex(Object object) {
        return calculationStrategy.hash(object);
    }

    private void next() {
        if (nextConsumerId >= ringbuffers.length - 1) {
            nextConsumerId = 0;
        } else {
            nextConsumerId++;
        }
    }

    @Override
    public void flush() {
        for (Consumer consumer : ringbuffers) {
             consumer.flush();
        }
    }

    @Override
    public boolean isFlushed() {
        boolean isFlushed = true;

        for (Consumer consumer : ringbuffers) {
            isFlushed &= consumer.isFlushed();
        }

        return isFlushed;
    }

    @Override
    public MemberDistributionStrategy getMemberDistributionStrategy() {
        return memberDistributionStrategy;
    }

    @Override
    public PartitioningStrategy getPartitionStrategy() {
        return calculationStrategy.getPartitioningStrategy();
    }

    @Override
    public HashingStrategy getHashingStrategy() {
        return calculationStrategy.getHashingStrategy();
    }

    @Override
    public boolean isShuffled() {
        boolean isShuffled = true;

        for (Consumer consumer : ringbuffers) {
            isShuffled &= consumer.isShuffled();
        }

        return isShuffled;
    }

    @Override
    public void open() {
        for (Consumer consumer : ringbuffers) {
            consumer.open();
        }
    }

    @Override
    public void close() {
        for (Consumer consumer : ringbuffers) {
            consumer.close();
        }
    }

    /**
     * @return parties of the composed actor
     */
    public Ringbuffer[] getRingbuffers() {
        return ringbuffers;
    }
}
