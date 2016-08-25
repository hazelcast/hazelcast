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
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.dag.Edge;
import com.hazelcast.jet.dag.Vertex;
import com.hazelcast.jet.data.io.ProducerInputStream;
import com.hazelcast.jet.impl.actor.ringbuffer.RingbufferWithReferenceStrategy;
import com.hazelcast.jet.impl.container.task.ContainerTask;
import com.hazelcast.jet.impl.data.io.ObjectIOStream;
import com.hazelcast.jet.impl.job.JobContext;
import com.hazelcast.jet.impl.strategy.SerializedHashingStrategy;
import com.hazelcast.jet.impl.util.JetUtil;
import com.hazelcast.jet.strategy.HashingStrategy;
import com.hazelcast.jet.strategy.MemberDistributionStrategy;
import com.hazelcast.partition.strategy.StringAndPartitionAwarePartitioningStrategy;
import com.hazelcast.spi.NodeEngine;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

@SuppressFBWarnings("EI_EXPOSE_REP")
public class RingbufferActor implements ObjectActor {
    private final Edge edge;
    private final Vertex vertex;
    private final ContainerTask sourceTask;
    private final Object[] producerChunk;
    private final Ringbuffer<Object> ringbuffer;
    private final ObjectIOStream<Object> flushBuffer;
    private final List<ProducerCompletionHandler> completionHandlers;
    private int producedCount;
    private int lastConsumedCount;
    private int currentFlushedCount;
    private volatile boolean isClosed;

    public RingbufferActor(NodeEngine nodeEngine,
                           JobContext jobContext,
                           ContainerTask sourceTask,
                           Vertex vertex) {
        this(nodeEngine, jobContext, sourceTask, vertex, null, false);
    }

    public RingbufferActor(NodeEngine nodeEngine,
                           JobContext jobContext,
                           ContainerTask sourceTask,
                           Vertex vertex,
                           Edge edge) {
        this(nodeEngine, jobContext, sourceTask, vertex, edge, true);
    }

    public RingbufferActor(NodeEngine nodeEngine,
                           JobContext jobContext,
                           ContainerTask sourceTask,
                           Vertex vertex,
                           Edge edge,
                           boolean registerListener) {
        this.edge = edge;
        this.sourceTask = sourceTask;
        this.vertex = vertex;
        JobConfig jobConfig = jobContext.getJobConfig();
        int objectChunkSize = jobConfig.getChunkSize();
        this.producerChunk = new Object[objectChunkSize];
        int ringbufferSize = jobConfig.getRingbufferSize();
        this.flushBuffer = new ObjectIOStream<>(new Object[objectChunkSize]);
        this.completionHandlers = new CopyOnWriteArrayList<>();
        this.ringbuffer = new RingbufferWithReferenceStrategy<>(ringbufferSize,
                nodeEngine.getLogger(RingbufferActor.class)
        );

        if (registerListener) {
            jobContext.registerJobListener(jobContext1 -> clear());
        }
    }

    public void clear() {
        Arrays.fill(this.producerChunk, null);
    }

    private int flushChunk() {
        if (this.currentFlushedCount >= this.flushBuffer.size()) {
            return 0;
        }

        int acquired = this.ringbuffer.acquire(this.flushBuffer.size() - this.currentFlushedCount);

        if (acquired <= 0) {
            return 0;
        }

        this.ringbuffer.commit(this.flushBuffer, this.currentFlushedCount);

        return acquired;
    }

    @Override
    public int consumeChunk(ProducerInputStream<Object> chunk) {
        this.currentFlushedCount = 0;
        this.lastConsumedCount = 0;
        this.flushBuffer.consumeStream(chunk);
        return chunk.size();
    }

    @Override
    public int consumeObject(Object object) {
        this.currentFlushedCount = 0;
        this.lastConsumedCount = 0;
        this.flushBuffer.consume(object);
        return 1;
    }

    @Override
    public int flush() {
        if (this.flushBuffer.size() > 0) {
            try {
                int flushed = flushChunk();
                this.lastConsumedCount = flushed;
                this.currentFlushedCount += flushed;
                return flushed;
            } catch (Exception e) {
                throw JetUtil.reThrow(e);
            }
        }

        return 0;
    }

    @Override
    public boolean isFlushed() {
        if (this.flushBuffer.size() == 0) {
            return true;
        }

        if (this.currentFlushedCount < this.flushBuffer.size()) {
            flush();
        }

        boolean flushed = this.currentFlushedCount >= this.flushBuffer.size();

        if (flushed) {
            this.currentFlushedCount = 0;
            this.flushBuffer.reset();
        }

        return flushed;
    }

    @Override
    public Object[] produce() {
        this.producedCount = this.ringbuffer.fetch(this.producerChunk);

        if (this.producedCount <= 0) {
            return null;
        }

        return this.producerChunk;
    }

    @Override
    public int lastProducedCount() {
        return this.producedCount;
    }

    @Override
    public ContainerTask getSourceTask() {
        return this.sourceTask;
    }

    @Override
    public void registerCompletionHandler(ProducerCompletionHandler runnable) {
        this.completionHandlers.add(runnable);
    }

    @Override
    public void handleProducerCompleted() {
        for (ProducerCompletionHandler handler : this.completionHandlers) {
            handler.onComplete(this);
        }
    }

    @Override
    public boolean isShuffled() {
        return false;
    }

    @Override
    public String getName() {
        return this.vertex.getName();
    }

    @Override
    public boolean isClosed() {
        return this.isClosed;
    }

    @Override
    public void open() {
        this.ringbuffer.reset();
        this.isClosed = false;
    }

    @Override
    public void close() {
        this.isClosed = true;
    }

    @Override
    public int lastConsumedCount() {
        return this.lastConsumedCount;
    }

    public MemberDistributionStrategy getMemberDistributionStrategy() {
        return this.edge == null ? null : this.edge.getMemberDistributionStrategy();
    }

    @Override
    public PartitioningStrategy getPartitionStrategy() {
        return this.edge == null ? StringAndPartitionAwarePartitioningStrategy.INSTANCE
                : this.edge.getPartitioningStrategy();
    }

    @Override
    public HashingStrategy getHashingStrategy() {
        return this.edge == null ? SerializedHashingStrategy.INSTANCE : this.edge.getHashingStrategy();
    }
}
