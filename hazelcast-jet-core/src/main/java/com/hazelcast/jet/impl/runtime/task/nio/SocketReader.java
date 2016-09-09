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

package com.hazelcast.jet.impl.runtime.task.nio;

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.impl.actor.RingbufferActor;
import com.hazelcast.jet.impl.data.io.IOBuffer;
import com.hazelcast.jet.impl.data.io.JetPacket;
import com.hazelcast.jet.impl.job.JobContext;
import com.hazelcast.jet.impl.runtime.JobManager;
import com.hazelcast.jet.impl.runtime.VertexRunner;
import com.hazelcast.jet.impl.runtime.task.VertexTask;
import com.hazelcast.jet.impl.util.BooleanHolder;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.NodeEngine;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.hazelcast.jet.impl.util.JetUtil.unchecked;

/**
 * Represents abstract task to read from network socket;
 *
 * The architecture is following:
 *
 *
 * <pre>
 *  SocketChannel (JetAddress)  -&gt; SocketReader -&gt; Consumer(ringBuffer)
 * </pre>
 */
public class SocketReader
        extends NetworkTask {
    protected volatile ByteBuffer receiveBuffer;
    protected boolean isBufferActive;

    private final int chunkSize;
    private final Address jetAddress;
    private final JobContext jobContext;
    private final IOBuffer<JetPacket> buffer;
    private final List<RingbufferActor> consumers = new ArrayList<RingbufferActor>();
    private final Map<Address, SocketWriter> writers = new HashMap<Address, SocketWriter>();

    private JetPacket packet;
    private volatile boolean socketAssigned;
    private final byte[] jobNameBytes;

    public SocketReader(JobContext jobContext,
                        Address jetAddress) {
        super(jobContext.getNodeEngine(), jetAddress);

        this.jetAddress = jetAddress;
        this.jobContext = jobContext;
        InternalSerializationService serializationService =
                (InternalSerializationService) jobContext.getNodeEngine().getSerializationService();
        this.jobNameBytes = serializationService.toBytes(this.jobContext.getName());
        this.chunkSize = jobContext.getJobConfig().getChunkSize();
        this.buffer = new IOBuffer<JetPacket>(new JetPacket[this.chunkSize]);
    }

    public SocketReader(NodeEngine nodeEngine) {
        super(nodeEngine, null);
        this.jetAddress = null;
        this.socketAssigned = true;
        this.jobContext = null;
        this.chunkSize = JobConfig.DEFAULT_CHUNK_SIZE;
        this.jobNameBytes = null;
        this.buffer = new IOBuffer<JetPacket>(new JetPacket[this.chunkSize]);
    }

    /**
     * Init task, perform initialization actions before task being executed
     * The strict rule is that this method will be executed synchronously on
     * all nodes in cluster before any real task's  execution
     */
    public void init() {
        super.init();
        socketAssigned = false;
        isBufferActive = false;
    }

    @Override
    public boolean onExecute(BooleanHolder payload) throws Exception {
        if (checkInterrupted()) {
            return false;
        }

        if (socketAssigned) {
            if (processRead(payload)) {
                return true;
            }
        } else {
            if (waitingForFinish) {
                finished = true;
            } else {
                return true;
            }
        }
        return checkFinished();
    }

    private boolean processRead(BooleanHolder payload) throws Exception {
        if (!isFlushed()) {
            payload.set(false);
            return true;
        }

        if (isBufferActive) {
            if (!readBuffer()) {
                return true;
            }
        }

        readSocket(payload);

        if (waitingForFinish) {
            if ((!payload.get()) && (isFlushed())) {
                finished = true;
            }
        }
        return false;
    }

    private boolean checkInterrupted() {
        if (destroyed) {
            closeSocket();
            return true;
        }

        if (interrupted) {
            closeSocket();
            finalized = true;
            notifyAMTaskFinished();
            return true;
        }
        return false;
    }

    @Override
    protected void notifyAMTaskFinished() {
        jobContext.getJobManager().notifyNetworkTaskFinished();
    }

    private boolean readSocket(BooleanHolder payload) {
        if ((socketChannel != null) && (socketChannel.isConnected())) {
            try {
                if (socketChannel != null) {
                    int readBytes = socketChannel.read(receiveBuffer);

                    if (readBytes <= 0) {
                        return handleEmptyChannel(payload, readBytes);
                    } else {
                        totalBytes += readBytes;
                    }
                }

                receiveBuffer.flip();
                readBuffer();
                payload.set(true);
            } catch (IOException e) {
                closeSocket();
            } catch (Exception e) {
                throw unchecked(e);
            }

            return true;
        } else {
            payload.set(false);
            return true;
        }
    }

    private boolean handleEmptyChannel(BooleanHolder payload, int readBytes) {
        if (readBytes < 0) {
            return false;
        }

        payload.set(false);
        return readBytes == 0;
    }

    protected boolean readBuffer() throws Exception {
        while (receiveBuffer.hasRemaining()) {
            if (packet == null) {
                packet = new JetPacket();
            }

            if (!packet.readFrom(receiveBuffer)) {
                alignBuffer(receiveBuffer);
                isBufferActive = false;
                return true;
            }

            // False means this is threadAcceptor
            if (!consumePacket(packet)) {
                packet = null;
                return false;
            }

            packet = null;

            if (buffer.size() >= chunkSize) {
                flush();

                if (!isFlushed()) {
                    isBufferActive = true;
                    return false;
                }
            }
        }

        isBufferActive = false;
        alignBuffer(receiveBuffer);
        flush();
        return isFlushed();
    }

    protected boolean alignBuffer(ByteBuffer buffer) {
        if (buffer.hasRemaining()) {
            buffer.compact();
            return true;
        } else {
            buffer.clear();
            return false;
        }
    }

    protected boolean consumePacket(JetPacket packet)  {
        buffer.collect(packet);
        return true;
    }

    private void flush() throws Exception {
        if (buffer.size() > 0) {
            for (JetPacket packet : buffer) {
                int header = resolvePacket(packet);

                if (header > 0) {
                    sendResponse(packet, header);
                }
            }

            buffer.reset();
        }
    }

    private void sendResponse(JetPacket jetPacket, int header) throws Exception {
        jetPacket.reset();
        jetPacket.setHeader(header);
        writers.get(jetAddress).sendServicePacket(jetPacket);
    }

    /**
     * @return - true of last write to the consumer-queue has been flushed;
     */
    public boolean isFlushed() {
        boolean isFlushed = true;

        for (int i = 0; i < consumers.size(); i++) {
            isFlushed &= consumers.get(i).isFlushed();
        }

        return isFlushed;
    }

    /**
     * Assign corresponding socketChannel to reader-task;
     *
     * @param socketChannel  - network socketChannel;
     * @param receiveBuffer  - byteBuffer to be used to read data from socket;
     * @param isBufferActive - true , if buffer can be used for read (not all data has been read), false otherwise
     */
    public void setSocketChannel(SocketChannel socketChannel,
                                 ByteBuffer receiveBuffer,
                                 boolean isBufferActive) {
        this.receiveBuffer = receiveBuffer;
        this.socketChannel = socketChannel;
        this.isBufferActive = isBufferActive;

        try {
            socketChannel.configureBlocking(false);
        } catch (IOException e) {
            throw unchecked(e);
        }

        socketAssigned = true;
    }

    /**
     * Register output ringBuffer consumer;
     *
     * @param ringbufferActor - corresponding output consumer;
     */
    public void registerConsumer(RingbufferActor ringbufferActor) {
        consumers.add(ringbufferActor);
    }

    /**
     * Assigns corresponding socket writer on the opposite node;
     *
     * @param writeAddress - JET's member node address;
     * @param socketWriter - SocketWriter task;
     */
    public void assignWriter(Address writeAddress,
                             SocketWriter socketWriter) {
        writers.put(writeAddress, socketWriter);
    }

    public int resolvePacket(JetPacket packet) throws Exception {
        int header = packet.getHeader();

        switch (header) {
            /*Request - bytes for pair chunk*/
            /*Request shuffling channel closed*/
            case JetPacket.HEADER_JET_DATA_CHUNK:
            case JetPacket.HEADER_JET_SHUFFLER_CLOSED:
            case JetPacket.HEADER_JET_DATA_CHUNK_SENT:
                return notifyShufflingReceiver(packet);

            case JetPacket.HEADER_JET_EXECUTION_ERROR:
                packet.setRemoteMember(jetAddress);
                jobContext.getJobManager().notifyRunners(packet);
                return 0;

            case JetPacket.HEADER_JET_DATA_NO_APP_FAILURE:
            case JetPacket.HEADER_JET_DATA_NO_TASK_FAILURE:
            case JetPacket.HEADER_JET_DATA_NO_MEMBER_FAILURE:
            case JetPacket.HEADER_JET_CHUNK_WRONG_CHUNK_FAILURE:
            case JetPacket.HEADER_JET_DATA_NO_VERTEX_RUNNER_FAILURE:
            case JetPacket.HEADER_JET_APPLICATION_IS_NOT_EXECUTING:
                invalidateAll();
                return 0;

            default:
                return 0;
        }
    }

    private void invalidateAll() throws Exception {
        for (SocketWriter sender : writers.values()) {
            JetPacket jetPacket = new JetPacket(
                    jobNameBytes
            );

            jetPacket.setHeader(JetPacket.HEADER_JET_EXECUTION_ERROR);
            sender.sendServicePacket(jetPacket);
        }
    }

    private int notifyShufflingReceiver(JetPacket packet) throws Exception {
        JobManager jobManager = jobContext.getJobManager();
        VertexRunner vertexRunner = jobManager.getRunnersMap().get(packet.getVertexRunnerId());

        if (vertexRunner == null) {
            logger.warning("No such vertex runner with vertexRunnerId="
                    + packet.getVertexRunnerId()
                    + " jetPacket="
                    + packet
                    + ". Job will be interrupted."
            );

            return JetPacket.HEADER_JET_DATA_NO_VERTEX_RUNNER_FAILURE;
        }

        VertexTask vertexTask = vertexRunner.getVertexMap().get(packet.getTaskId());

        if (vertexTask == null) {
            logger.warning("No such task in vertexRunner with vertexRunnerId="
                    + packet.getVertexRunnerId()
                    + " taskId="
                    + packet.getTaskId()
                    + " jetPacket="
                    + packet
                    + ". Job will be interrupted."
            );

            return JetPacket.HEADER_JET_DATA_NO_TASK_FAILURE;
        }

        vertexTask.getShufflingReceiver(jetAddress).consume(packet);
        return 0;
    }
}
