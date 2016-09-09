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
import com.hazelcast.jet.Edge;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.impl.actor.ringbuffer.RingbufferWithReferenceStrategy;
import com.hazelcast.jet.impl.data.io.IOBuffer;
import com.hazelcast.jet.impl.job.JobContext;
import com.hazelcast.jet.impl.runtime.task.VertexTask;
import com.hazelcast.jet.impl.strategy.SerializedHashingStrategy;
import com.hazelcast.jet.runtime.InputChunk;
import com.hazelcast.jet.strategy.HashingStrategy;
import com.hazelcast.jet.strategy.MemberDistributionStrategy;
import com.hazelcast.partition.strategy.StringAndPartitionAwarePartitioningStrategy;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static com.hazelcast.jet.impl.util.JetUtil.unchecked;

@SuppressFBWarnings("EI_EXPOSE_REP")
public class RingbufferActor implements Actor {
    private final Edge edge;
    private final VertexTask sourceTask;
    private final Object[] producerChunk;
    private final Ringbuffer<Object> ringbuffer;
    private final IOBuffer<Object> flushBuffer;
    private final List<ProducerCompletionHandler> completionHandlers;
    private int producedCount;
    private int lastConsumedCount;
    private int currentFlushedCount;
    private volatile boolean isClosed;

    public RingbufferActor(VertexTask sourceTask) {
        this(sourceTask, null, false);
    }

    public RingbufferActor(VertexTask sourceTask,
                           Edge edge) {
        this(sourceTask, edge, true);
    }

    public RingbufferActor(VertexTask sourceTask,
                           Edge edge,
                           boolean registerListener) {
        this.edge = edge;
        this.sourceTask = sourceTask;
        JobContext jobContext = sourceTask.getTaskContext().getJobContext();
        JobConfig jobConfig = jobContext.getJobConfig();
        int objectChunkSize = jobConfig.getChunkSize();
        this.producerChunk = new Object[objectChunkSize];
        int ringbufferSize = jobConfig.getRingbufferSize();
        this.flushBuffer = new IOBuffer<>(new Object[objectChunkSize]);
        this.completionHandlers = new CopyOnWriteArrayList<>();
        this.ringbuffer = new RingbufferWithReferenceStrategy<>(ringbufferSize,
                jobContext.getNodeEngine().getLogger(RingbufferActor.class)
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
    public int consume(InputChunk<Object> chunk) {
        this.currentFlushedCount = 0;
        this.lastConsumedCount = 0;
        this.flushBuffer.collect(chunk);
        return chunk.size();
    }

    @Override
    public int consume(Object object) {
        this.currentFlushedCount = 0;
        this.lastConsumedCount = 0;
        this.flushBuffer.collect(object);
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
                throw unchecked(e);
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
    public VertexTask getSourceTask() {
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
        return sourceTask.getVertex().getName();
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
