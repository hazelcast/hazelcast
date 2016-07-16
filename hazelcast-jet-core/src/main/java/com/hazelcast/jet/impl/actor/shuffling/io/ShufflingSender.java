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

package com.hazelcast.jet.impl.actor.shuffling.io;

import com.hazelcast.core.PartitioningStrategy;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.ObjectDataOutputStream;
import com.hazelcast.jet.data.io.ProducerInputStream;
import com.hazelcast.jet.impl.actor.RingBufferActor;
import com.hazelcast.jet.impl.container.ContainerContext;
import com.hazelcast.jet.impl.container.ContainerTask;
import com.hazelcast.jet.impl.dag.sink.AbstractHazelcastWriter;
import com.hazelcast.jet.impl.data.io.JetPacket;
import com.hazelcast.jet.impl.util.JetUtil;
import com.hazelcast.jet.io.IOContext;
import com.hazelcast.nio.Address;
import com.hazelcast.partition.strategy.StringPartitioningStrategy;
import com.hazelcast.spi.impl.NodeEngineImpl;

public class ShufflingSender extends AbstractHazelcastWriter {
    private final int taskID;
    private final Address address;

    private final int containerID;
    private final byte[] jobNameBytes;
    private final RingBufferActor ringBufferActor;
    private final ChunkedOutputStream serializer;
    private final SenderObjectWriter senderObjectWriter;
    private final ObjectDataOutputStream dataOutputStream;
    private final IOContext ioContext;
    private volatile boolean closed;

    public ShufflingSender(ContainerContext containerContext, int taskID, ContainerTask containerTask, Address address) {
        super(containerContext, -1);
        this.taskID = taskID;
        this.address = address;
        NodeEngineImpl nodeEngine = (NodeEngineImpl) containerContext.getNodeEngine();
        this.containerID = containerContext.getID();
        String jobName = containerContext.getJobContext().getName();
        this.jobNameBytes = ((InternalSerializationService) nodeEngine.getSerializationService()).toBytes(jobName);
        this.ringBufferActor = new RingBufferActor(nodeEngine, containerContext.getJobContext(), containerTask,
                containerContext.getVertex());
        this.serializer = new ChunkedOutputStream(this.ringBufferActor, containerContext, taskID);
        this.ioContext = containerTask.getTaskContext().getIoContext();
        this.dataOutputStream = new ObjectDataOutputStream(
                this.serializer, (InternalSerializationService) nodeEngine.getSerializationService());
        this.senderObjectWriter = new SenderObjectWriter(ioContext);
    }

    @Override
    public int consumeChunk(ProducerInputStream<Object> chunk) throws Exception {
        senderObjectWriter.write(chunk, dataOutputStream);
        serializer.flushSender();
        JetPacket packet = new JetPacket(taskID, containerID, jobNameBytes);
        packet.setHeader(JetPacket.HEADER_JET_DATA_CHUNK_SENT);
        ringBufferActor.consumeObject(packet);
        return chunk.size();
    }

    @Override
    public int flush() {
        if (chunkBuffer.size() > 0) {
            try {
                consumeChunk(chunkBuffer);
            } catch (Exception e) {
                throw JetUtil.reThrow(e);
            }
            chunkBuffer.reset();
        }
        return ringBufferActor.flush();
    }

    @Override
    public boolean isFlushed() {
        boolean result = ringBufferActor.isFlushed();
        checkClosed(result);
        return result;
    }

    private void checkClosed(boolean result) {
        if ((result) && (closed)) {
            ringBufferActor.close();
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
        ringBufferActor.open();
    }

    @Override
    protected void processChunk(ProducerInputStream<Object> chunk) {
        try {
            consumeChunk(chunk);
        } catch (Exception e) {
            throw JetUtil.reThrow(e);
        }
    }

    @Override
    public PartitioningStrategy getPartitionStrategy() {
        return StringPartitioningStrategy.INSTANCE;
    }

    @Override
    public boolean isPartitioned() {
        return false;
    }

    private void writePacket(JetPacket packet) {
        try {
            ringBufferActor.consumeObject(packet);
            ringBufferActor.flush();
        } catch (Exception e) {
            throw JetUtil.reThrow(e);
        }
    }

    @Override
    public void close() {
        if (!closed) {
            try {
                JetPacket packet = new JetPacket(taskID, containerID, jobNameBytes);
                packet.setHeader(JetPacket.HEADER_JET_SHUFFLER_CLOSED);
                writePacket(packet);
            } finally {
                closed = true;
            }
        }
    }

    public RingBufferActor getRingBufferActor() {
        return ringBufferActor;
    }
}
