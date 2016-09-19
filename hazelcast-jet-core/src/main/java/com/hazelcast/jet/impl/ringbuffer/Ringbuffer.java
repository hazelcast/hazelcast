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
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.impl.data.io.IOBuffer;
import com.hazelcast.jet.impl.job.JobContext;
import com.hazelcast.jet.strategy.SerializedHashingStrategy;
import com.hazelcast.jet.runtime.Consumer;
import com.hazelcast.jet.runtime.InputChunk;
import com.hazelcast.jet.runtime.Producer;
import com.hazelcast.jet.runtime.ProducerCompletionHandler;
import com.hazelcast.jet.strategy.HashingStrategy;
import com.hazelcast.jet.strategy.MemberDistributionStrategy;
import com.hazelcast.partition.strategy.StringAndPartitionAwarePartitioningStrategy;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static com.hazelcast.jet.impl.util.JetUtil.unchecked;

@SuppressFBWarnings("EI_EXPOSE_REP")
public class Ringbuffer implements Consumer, Producer {
    private final Edge edge;
    private final String name;
    private final Object[] producerChunk;
    private final RingbufferIO<Object> ringbuffer;
    private final IOBuffer<Object> flushBuffer;
    private final List<ProducerCompletionHandler> completionHandlers;
    private int producedCount;
    private int lastConsumedCount;
    private int currentFlushedCount;

    public Ringbuffer(String name, JobContext jobContext) {
        this(name, jobContext, null, false);
    }

    public Ringbuffer(String name, JobContext jobContext, Edge edge) {
        this(name, jobContext, edge, true);
    }

    public Ringbuffer(String name, JobContext jobContext, Edge edge, boolean registerListener) {
        this.edge = edge;
        this.name = name;
        JobConfig jobConfig = jobContext.getJobConfig();
        int objectChunkSize = jobConfig.getChunkSize();
        this.producerChunk = new Object[objectChunkSize];
        int ringbufferSize = jobConfig.getRingbufferSize();
        this.flushBuffer = new IOBuffer<>(new Object[objectChunkSize]);
        this.completionHandlers = new CopyOnWriteArrayList<>();
        this.ringbuffer = new RingbufferWithReferenceStrategy<>(ringbufferSize,
                jobContext.getNodeEngine().getLogger(Ringbuffer.class)
        );

        if (registerListener) {
            jobContext.registerJobListener(jobContext1 -> clear());
        }
    }

    public void clear() {
        Arrays.fill(producerChunk, null);
    }

    private int flushChunk() {
        if (currentFlushedCount >= flushBuffer.size()) {
            return 0;
        }

        int acquired = ringbuffer.acquire(flushBuffer.size() - currentFlushedCount);

        if (acquired <= 0) {
            return 0;
        }

        ringbuffer.commit(flushBuffer, currentFlushedCount);

        return acquired;
    }

    @Override
    public int consume(InputChunk<Object> chunk) {
        currentFlushedCount = 0;
        lastConsumedCount = 0;
        flushBuffer.collect(chunk);
        return chunk.size();
    }

    @Override
    public int consume(Object object) {
        currentFlushedCount = 0;
        lastConsumedCount = 0;
        flushBuffer.collect(object);
        return 1;
    }

    @Override
    public int flush() {
        if (flushBuffer.size() > 0) {
            try {
                int flushed = flushChunk();
                lastConsumedCount = flushed;
                currentFlushedCount += flushed;
                return flushed;
            } catch (Exception e) {
                throw unchecked(e);
            }
        }

        return 0;
    }

    @Override
    public boolean isFlushed() {
        if (flushBuffer.size() == 0) {
            return true;
        }

        if (currentFlushedCount < flushBuffer.size()) {
            flush();
        }

        boolean flushed = currentFlushedCount >= flushBuffer.size();

        if (flushed) {
            currentFlushedCount = 0;
            flushBuffer.reset();
        }

        return flushed;
    }

    @Override
    public Object[] produce() {
        producedCount = ringbuffer.fetch(producerChunk);

        if (producedCount <= 0) {
            return null;
        }

        return producerChunk;
    }

    @Override
    public int lastProducedCount() {
        return producedCount;
    }

    @Override
    public void registerCompletionHandler(ProducerCompletionHandler runnable) {
        completionHandlers.add(runnable);
    }

    @Override
    public boolean isShuffled() {
        return !edge.isLocal();
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public void open() {
        ringbuffer.reset();
    }

    @Override
    public void close() {
        for (ProducerCompletionHandler handler : completionHandlers) {
            handler.onComplete(this);
        }
    }

    @Override
    public int lastConsumedCount() {
        return lastConsumedCount;
    }

    public MemberDistributionStrategy getMemberDistributionStrategy() {
        return edge == null ? null : edge.getMemberDistributionStrategy();
    }

    @Override
    public PartitioningStrategy getPartitionStrategy() {
        return edge == null ? StringAndPartitionAwarePartitioningStrategy.INSTANCE
                : edge.getPartitioningStrategy();
    }

    @Override
    public HashingStrategy getHashingStrategy() {
        return edge == null ? SerializedHashingStrategy.INSTANCE : edge.getHashingStrategy();
    }
}
