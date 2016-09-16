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
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.ObjectDataOutputStream;
import com.hazelcast.jet.impl.dag.sink.AbstractHazelcastWriter;
import com.hazelcast.jet.impl.data.io.JetPacket;
import com.hazelcast.jet.impl.job.JobContext;
import com.hazelcast.jet.impl.runtime.task.VertexTask;
import com.hazelcast.jet.io.SerializationOptimizer;
import com.hazelcast.jet.runtime.InputChunk;
import com.hazelcast.nio.Address;
import com.hazelcast.partition.strategy.StringAndPartitionAwarePartitioningStrategy;
import com.hazelcast.spi.impl.NodeEngineImpl;

import java.io.IOException;

import static com.hazelcast.jet.impl.util.JetUtil.unchecked;

public class ShufflingSender extends AbstractHazelcastWriter {
    private final int vertexRunnerId;
    private final int taskID;
    private final Address address;

    private final byte[] jobNameBytes;
    private final Ringbuffer ringbuffer;
    private final ChunkedOutputStream serializer;
    private final ObjectDataOutputStream dataOutputStream;
    private final SerializationOptimizer optimizer;
    private volatile boolean closed;

    public ShufflingSender(VertexTask task, int vertexRunnerId, Address address) {
        super(task.getTaskContext().getJobContext(), -1);
        this.vertexRunnerId = vertexRunnerId;
        this.taskID = task.getTaskContext().getTaskNumber();
        this.address = address;
        JobContext jobContext = task.getTaskContext().getJobContext();
        NodeEngineImpl nodeEngine = (NodeEngineImpl) jobContext.getNodeEngine();
        String jobName = jobContext.getName();
        this.jobNameBytes = ((InternalSerializationService) nodeEngine.getSerializationService()).toBytes(jobName);
        this.ringbuffer = new Ringbuffer(task.getVertex().getName(), jobContext);
        this.serializer = new ChunkedOutputStream(this.ringbuffer, task.getTaskContext(),
                vertexRunnerId, task.getTaskContext().getTaskNumber());
        this.optimizer = task.getTaskContext().getSerializationOptimizer();
        this.dataOutputStream = new ObjectDataOutputStream(
                this.serializer, (InternalSerializationService) nodeEngine.getSerializationService());
    }

    @Override
    public int consume(InputChunk<Object> chunk) {
        try {
            dataOutputStream.writeInt(chunk.size());
            for (Object object : chunk) {
                optimizer.write(object, dataOutputStream);
            }
        } catch (IOException e) {
            throw unchecked(e);
        }
        serializer.flushSender();
        JetPacket packet = new JetPacket(taskID, vertexRunnerId, jobNameBytes);
        packet.setHeader(JetPacket.HEADER_JET_DATA_CHUNK_SENT);
        ringbuffer.consume(packet);
        return chunk.size();
    }

    @Override
    public int flush() {
        if (chunkBuffer.size() > 0) {
            try {
                consume(chunkBuffer);
            } catch (Exception e) {
                throw unchecked(e);
            }
            chunkBuffer.reset();
        }
        return ringbuffer.flush();
    }

    @Override
    public boolean isFlushed() {
        boolean result = ringbuffer.isFlushed();
        checkClosed(result);
        return result;
    }

    private void checkClosed(boolean result) {
        if ((result) && (closed)) {
            ringbuffer.close();
        }
    }

    @Override
    public void open() {
        onOpen();
    }

    @Override
    protected void onOpen() {
        closed = false;
        isFlushed = true;
        ringbuffer.open();
    }

    @Override
    protected void processChunk(InputChunk<Object> inputChunk) {
        try {
            consume(inputChunk);
        } catch (Exception e) {
            throw unchecked(e);
        }
    }

    @Override
    public PartitioningStrategy getPartitionStrategy() {
        return StringAndPartitionAwarePartitioningStrategy.INSTANCE;
    }

    @Override
    public boolean isPartitioned() {
        return false;
    }

    private void writePacket(JetPacket packet) {
        try {
            ringbuffer.consume(packet);
            ringbuffer.flush();
        } catch (Exception e) {
            throw unchecked(e);
        }
    }

    @Override
    public void close() {
        if (!closed) {
            try {
                JetPacket packet = new JetPacket(taskID, vertexRunnerId, jobNameBytes);
                packet.setHeader(JetPacket.HEADER_JET_SHUFFLER_CLOSED);
                writePacket(packet);
            } finally {
                closed = true;
            }
        }
    }

    public Ringbuffer getRingbuffer() {
        return ringbuffer;
    }
}
