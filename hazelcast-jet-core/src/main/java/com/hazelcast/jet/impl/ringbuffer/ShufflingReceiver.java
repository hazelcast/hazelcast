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

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.ObjectDataInputStream;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.impl.data.io.IOBuffer;
import com.hazelcast.jet.impl.data.io.JetPacket;
import com.hazelcast.jet.impl.job.JobContext;
import com.hazelcast.jet.impl.runtime.task.VertexTask;
import com.hazelcast.jet.impl.util.JetUtil;
import com.hazelcast.jet.io.SerializationOptimizer;
import com.hazelcast.jet.runtime.Producer;
import com.hazelcast.jet.runtime.ProducerCompletionHandler;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.spi.impl.NodeEngineImpl;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

public class ShufflingReceiver implements Producer {

    private final ObjectDataInput in;
    private final List<ProducerCompletionHandler> handlers = new CopyOnWriteArrayList<>();
    private final ChunkedInputStream chunkReceiver;
    private final Ringbuffer ringbuffer;
    private final IOBuffer<JetPacket> packetBuffers;
    private final SerializationOptimizer optimizer;
    private final String name;

    private Object[] dataChunkBuffer;
    private Object[] packets;
    private int lastPacketIdx;
    private int lastProducedPacketsCount;

    private volatile boolean closed;
    private volatile int lastProducedCount;
    private volatile int dataChunkLength = -1;
    private volatile boolean finalized;


    public ShufflingReceiver(VertexTask vertexTask) {
        JobContext jobContext = vertexTask.getTaskContext().getJobContext();
        NodeEngineImpl nodeEngine = (NodeEngineImpl) jobContext.getNodeEngine();
        JobConfig jobConfig = jobContext.getJobConfig();
        int chunkSize = jobConfig.getChunkSize();
        this.ringbuffer = new Ringbuffer(vertexTask.getVertex().getName(), jobContext);
        this.packetBuffers = new IOBuffer<>(new JetPacket[chunkSize]);
        this.chunkReceiver = new ChunkedInputStream(this.packetBuffers);
        this.in = new ObjectDataInputStream(chunkReceiver,
                (InternalSerializationService) nodeEngine.getSerializationService());
        this.name = vertexTask.getVertex().getName();
        optimizer = vertexTask.getTaskContext().getSerializationOptimizer();
    }

    @Override
    public void open() {
        closed = false;
        finalized = false;
        chunkReceiver.onOpen();
        ringbuffer.open();
    }

    @Override
    public void close() {
        closed = true;
        finalized = true;
        ringbuffer.close();

        for (ProducerCompletionHandler handler : handlers) {
            handler.onComplete(this);
        }
    }

    @Override
    public Object[] produce() throws Exception {
        if (closed) {
            return null;
        }
        if (packets != null) {
            return processPackets();
        }
        packets = ringbuffer.produce();
        lastProducedPacketsCount = ringbuffer.lastProducedCount();
        if (JetUtil.isEmpty(packets)) {
            if (finalized) {
                close();

            }
            return null;
        }
        return processPackets();
    }

    @Override
    public int lastProducedCount() {
        return lastProducedCount;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public void registerCompletionHandler(ProducerCompletionHandler runnable) {
        handlers.add(runnable);
    }

    public boolean consume(JetPacket packet) {
        ringbuffer.consume(packet);
        return true;
    }

    public Ringbuffer getRingbuffer() {
        return ringbuffer;
    }

    private Object[] processPackets() throws Exception {
        for (int i = lastPacketIdx; i < lastProducedPacketsCount; i++) {
            JetPacket packet = (JetPacket) packets[i];
            if (packet.getHeader() == JetPacket.HEADER_JET_DATA_CHUNK_SENT) {
                deserializePackets();
                if (i == lastProducedPacketsCount - 1) {
                    reset();
                } else {
                    lastPacketIdx = i + 1;
                }
                return dataChunkBuffer;
            } else if (packet.getHeader() == JetPacket.HEADER_JET_SHUFFLER_CLOSED) {
                finalized = true;
            } else {
                packetBuffers.collect(packet);
            }
        }
        reset();
        return null;
    }

    private void reset() {
        this.packets = null;
        lastPacketIdx = 0;
        lastProducedPacketsCount = 0;
    }

    private void deserializePackets() throws IOException {
        if (dataChunkLength == -1) {
            dataChunkLength = in.readInt();
            dataChunkBuffer = new Object[dataChunkLength];
        }
        try {
            for (int i = 0; i < dataChunkLength; i++) {
                dataChunkBuffer[i] = optimizer.read(in);
            }
            lastProducedCount = dataChunkLength;
        } finally {
            dataChunkLength = -1;
        }
        packetBuffers.reset();
    }
}
