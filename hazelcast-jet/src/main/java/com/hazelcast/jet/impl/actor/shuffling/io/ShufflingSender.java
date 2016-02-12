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
import com.hazelcast.internal.serialization.impl.ObjectDataOutputStream;
import com.hazelcast.jet.api.container.ContainerContext;
import com.hazelcast.jet.api.container.ContainerTask;
import com.hazelcast.jet.api.data.io.ProducerInputStream;
import com.hazelcast.jet.impl.actor.RingBufferActor;
import com.hazelcast.jet.impl.dag.tap.sink.AbstractHazelcastWriter;
import com.hazelcast.jet.impl.hazelcast.JetPacket;
import com.hazelcast.jet.impl.util.JetUtil;
import com.hazelcast.jet.spi.dag.tap.SinkTapWriteStrategy;
import com.hazelcast.jet.spi.data.io.ObjectWriterFactory;
import com.hazelcast.nio.Address;
import com.hazelcast.partition.strategy.StringPartitioningStrategy;
import com.hazelcast.spi.impl.NodeEngineImpl;

public class ShufflingSender extends AbstractHazelcastWriter {
    private final int taskID;
    private final Address address;

    private final int containerID;
    private final byte[] applicationName;
    private final RingBufferActor ringBufferActor;
    private final ChunkedOutputStream serializer;
    private final SenderObjectWriter senderObjectWriter;
    private final ObjectDataOutputStream dataOutputStream;
    private final ObjectWriterFactory objectWriterFactory;
    private final ContainerContext containerContext;
    private volatile boolean closed;

    public ShufflingSender(ContainerContext containerContext,
                           int taskID,
                           ContainerTask containerTask, Address address) {
        super(containerContext, -1, SinkTapWriteStrategy.CLEAR_AND_REPLACE);

        this.taskID = taskID;
        this.address = address;
        NodeEngineImpl nodeEngine = (NodeEngineImpl) containerContext.getNodeEngine();
        this.containerID = containerContext.getID();
        this.applicationName = containerContext.getApplicationContext().getName().getBytes();

        this.containerContext = containerContext;

        this.ringBufferActor = new RingBufferActor(
                nodeEngine,
                containerContext.getApplicationContext(),
                containerTask,
                containerContext.getVertex()
        );

        this.serializer = new ChunkedOutputStream(
                this.ringBufferActor,
                containerContext,
                taskID
        );

        this.objectWriterFactory = containerTask.getTaskContext().getObjectWriterFactory();

        this.dataOutputStream = new ObjectDataOutputStream(
                this.serializer,
                nodeEngine.getSerializationService()
        );

        this.senderObjectWriter = new SenderObjectWriter(this.objectWriterFactory);
    }

    @Override
    public int consumeChunk(ProducerInputStream<Object> chunk) throws Exception {
        this.senderObjectWriter.write(
                chunk,
                this.dataOutputStream,
                this.objectWriterFactory
        );

        this.serializer.flushSender();

        JetPacket packet = new JetPacket(
                this.taskID,
                this.containerID,
                this.applicationName
        );

        packet.setHeader(JetPacket.HEADER_JET_DATA_CHUNK_SENT);
        this.ringBufferActor.consumeObject(packet);

        return chunk.size();
    }

    @Override
    public int flush() {
        if (this.chunkBuffer.size() > 0) {
            try {
                consumeChunk(this.chunkBuffer);
            } catch (Exception e) {
                throw JetUtil.reThrow(e);
            }

            this.chunkBuffer.reset();
        }

        return this.ringBufferActor.flush();
    }

    @Override
    public boolean isFlushed() {
        boolean result = this.ringBufferActor.isFlushed();
        checkClosed(result);
        return result;
    }

    private void checkClosed(boolean result) {
        if ((result) && (this.closed)) {
            this.ringBufferActor.close();
        }
    }

    @Override
    public void open() {
        onOpen();
    }

    @Override
    protected void onOpen() {
        this.closed = false;
        this.isFlushed = true;
        this.ringBufferActor.open();
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
            this.ringBufferActor.consumeObject(packet);
            this.ringBufferActor.flush();
        } catch (Exception e) {
            throw JetUtil.reThrow(e);
        }
    }

    @Override
    public void close() {
        if (!this.closed) {
            try {
                JetPacket packet = new JetPacket(
                        this.taskID,
                        this.containerID,
                        this.applicationName
                );

                packet.setHeader(JetPacket.HEADER_JET_SHUFFLER_CLOSED);
                writePacket(packet);
            } finally {
                this.closed = true;
            }
        }
    }

    public RingBufferActor getRingBufferActor() {
        return this.ringBufferActor;
    }
}
