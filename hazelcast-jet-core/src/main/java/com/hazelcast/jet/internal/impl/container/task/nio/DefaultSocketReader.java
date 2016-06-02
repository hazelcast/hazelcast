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

package com.hazelcast.jet.internal.impl.container.task.nio;

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.jet.internal.api.application.ApplicationContext;
import com.hazelcast.jet.internal.api.container.ContainerTask;
import com.hazelcast.jet.internal.api.container.ProcessingContainer;
import com.hazelcast.jet.internal.api.container.applicationmaster.ApplicationMaster;
import com.hazelcast.jet.internal.api.data.io.SocketReader;
import com.hazelcast.jet.internal.api.data.io.SocketWriter;
import com.hazelcast.jet.internal.api.executor.Payload;
import com.hazelcast.jet.internal.impl.actor.RingBufferActor;
import com.hazelcast.jet.internal.impl.data.io.DefaultObjectIOStream;
import com.hazelcast.jet.internal.impl.hazelcast.JetPacket;
import com.hazelcast.jet.internal.impl.util.JetUtil;
import com.hazelcast.jet.api.config.JetApplicationConfig;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.NodeEngine;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DefaultSocketReader
        extends AbstractNetworkTask implements SocketReader {
    protected volatile ByteBuffer receiveBuffer;
    protected boolean isBufferActive;

    private final int chunkSize;
    private final Address jetAddress;
    private final ApplicationContext applicationContext;
    private final DefaultObjectIOStream<JetPacket> buffer;
    private final List<RingBufferActor> consumers = new ArrayList<RingBufferActor>();
    private final Map<Address, SocketWriter> writers = new HashMap<Address, SocketWriter>();

    private JetPacket packet;
    private volatile boolean socketAssigned;
    private final byte[] applicationNameBytes;

    public DefaultSocketReader(ApplicationContext applicationContext,
                               Address jetAddress) {
        super(applicationContext.getNodeEngine(), jetAddress);

        this.jetAddress = jetAddress;
        this.applicationContext = applicationContext;
        InternalSerializationService serializationService =
                (InternalSerializationService) applicationContext.getNodeEngine().getSerializationService();
        this.applicationNameBytes = serializationService.toBytes(this.applicationContext.getName());
        this.chunkSize = applicationContext.getJetApplicationConfig().getChunkSize();
        this.buffer = new DefaultObjectIOStream<JetPacket>(new JetPacket[this.chunkSize]);
    }

    public DefaultSocketReader(NodeEngine nodeEngine) {
        super(nodeEngine, null);
        this.jetAddress = null;
        this.socketAssigned = true;
        this.applicationContext = null;
        this.chunkSize = JetApplicationConfig.DEFAULT_CHUNK_SIZE;
        this.applicationNameBytes = null;
        this.buffer = new DefaultObjectIOStream<JetPacket>(new JetPacket[this.chunkSize]);
    }

    public void init() {
        super.init();

        this.socketAssigned = false;
        this.isBufferActive = false;
    }

    @Override
    public boolean onExecute(Payload payload) throws Exception {
        if (checkInterrupted()) {
            return false;
        }

        if (!this.socketAssigned) {
            return true;
        }

        if (processRead(payload)) {
            return true;
        }

        return checkFinished();
    }

    private boolean processRead(Payload payload) throws Exception {
        if (!isFlushed()) {
            payload.set(false);
            return true;
        }

        if (this.isBufferActive) {
            if (!readBuffer()) {
                return true;
            }
        }

        readSocket(payload);

        if (this.waitingForFinish) {
            if ((!payload.produced()) && (isFlushed())) {
                this.finished = true;
            }
        }
        return false;
    }

    private boolean checkInterrupted() {
        if (this.destroyed) {
            closeSocket();
            return true;
        }

        if (this.interrupted) {
            closeSocket();
            this.finalized = true;
            notifyAMTaskFinished();
            return true;
        }
        return false;
    }

    @Override
    protected void notifyAMTaskFinished() {
        this.applicationContext.getApplicationMaster().notifyNetworkTaskFinished();
    }

    private boolean readSocket(Payload payload) {
        if ((this.socketChannel != null) && (this.socketChannel.isConnected())) {
            try {
                SocketChannel socketChannel = this.socketChannel;

                if (socketChannel != null) {
                    int readBytes = socketChannel.read(this.receiveBuffer);

                    if (readBytes <= 0) {
                        return handleEmptyChannel(payload, readBytes);
                    } else {
                        this.totalBytes += readBytes;
                    }
                }

                this.receiveBuffer.flip();
                readBuffer();
                payload.set(true);
            } catch (IOException e) {
                closeSocket();
            } catch (Exception e) {
                throw JetUtil.reThrow(e);
            }

            return true;
        } else {
            payload.set(false);
            return true;
        }
    }

    private boolean handleEmptyChannel(Payload payload, int readBytes) {
        if (readBytes < 0) {
            return false;
        }

        payload.set(false);
        return readBytes == 0;
    }

    protected boolean readBuffer() throws Exception {
        while (this.receiveBuffer.hasRemaining()) {
            if (this.packet == null) {
                this.packet = new JetPacket();
            }

            if (!this.packet.readFrom(this.receiveBuffer)) {
                alignBuffer(this.receiveBuffer);
                this.isBufferActive = false;
                return true;
            }

            // False means this is threadAcceptor
            if (!consumePacket(this.packet)) {
                this.packet = null;
                return false;
            }

            this.packet = null;

            if (this.buffer.size() >= this.chunkSize) {
                flush();

                if (!isFlushed()) {
                    this.isBufferActive = true;
                    return false;
                }
            }
        }

        this.isBufferActive = false;
        alignBuffer(this.receiveBuffer);
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

    protected boolean consumePacket(JetPacket packet) throws Exception {
        this.buffer.consume(packet);
        return true;
    }

    private void flush() throws Exception {
        if (this.buffer.size() > 0) {
            for (JetPacket packet : this.buffer) {
                int header = resolvePacket(packet);

                if (header > 0) {
                    sendResponse(packet, header);
                }
            }

            this.buffer.reset();
        }
    }

    private void sendResponse(JetPacket jetPacket, int header) throws Exception {
        jetPacket.reset();
        jetPacket.setHeader(header);
        this.writers.get(this.jetAddress).sendServicePacket(jetPacket);
    }

    @Override
    public boolean isFlushed() {
        boolean isFlushed = true;

        for (int i = 0; i < this.consumers.size(); i++) {
            isFlushed &= this.consumers.get(i).isFlushed();
        }

        return isFlushed;
    }

    @Override
    public void setSocketChannel(SocketChannel socketChannel,
                                 ByteBuffer receiveBuffer,
                                 boolean isBufferActive) {
        this.receiveBuffer = receiveBuffer;
        this.socketChannel = socketChannel;
        this.isBufferActive = isBufferActive;

        try {
            socketChannel.configureBlocking(false);
        } catch (IOException e) {
            throw JetUtil.reThrow(e);
        }

        this.socketAssigned = true;
    }

    @Override
    public void registerConsumer(RingBufferActor ringBufferActor) {
        this.consumers.add(ringBufferActor);
    }

    @Override
    public void assignWriter(Address writeAddress,
                             SocketWriter socketWriter) {
        this.writers.put(writeAddress, socketWriter);
    }

    public int resolvePacket(JetPacket packet) throws Exception {
        int header = packet.getHeader();

        switch (header) {
            /*Request - bytes for tuple chunk*/
            /*Request shuffling channel closed*/
            case JetPacket.HEADER_JET_DATA_CHUNK:
            case JetPacket.HEADER_JET_SHUFFLER_CLOSED:
            case JetPacket.HEADER_JET_DATA_CHUNK_SENT:
                return notifyShufflingReceiver(packet);

            case JetPacket.HEADER_JET_EXECUTION_ERROR:
                packet.setRemoteMember(this.jetAddress);
                this.applicationContext.getApplicationMaster().notifyContainers(packet);
                return 0;

            case JetPacket.HEADER_JET_DATA_NO_APP_FAILURE:
            case JetPacket.HEADER_JET_DATA_NO_TASK_FAILURE:
            case JetPacket.HEADER_JET_DATA_NO_MEMBER_FAILURE:
            case JetPacket.HEADER_JET_CHUNK_WRONG_CHUNK_FAILURE:
            case JetPacket.HEADER_JET_DATA_NO_CONTAINER_FAILURE:
            case JetPacket.HEADER_JET_APPLICATION_IS_NOT_EXECUTING:
                invalidateAll();
                return 0;

            default:
                return 0;
        }
    }

    private void invalidateAll() throws Exception {
        for (SocketWriter sender : this.writers.values()) {
            JetPacket jetPacket = new JetPacket(
                    this.applicationNameBytes
            );

            jetPacket.setHeader(JetPacket.HEADER_JET_EXECUTION_ERROR);
            sender.sendServicePacket(jetPacket);
        }
    }

    private int notifyShufflingReceiver(JetPacket packet) throws Exception {
        ApplicationMaster applicationMaster = this.applicationContext.getApplicationMaster();
        ProcessingContainer processingContainer = applicationMaster.getContainersCache().get(packet.getContainerId());

        if (processingContainer == null) {
            this.logger.warning("No such container with containerId="
                            + packet.getContainerId()
                            + " jetPacket="
                            + packet
                            + ". Application will be interrupted."
            );

            return JetPacket.HEADER_JET_DATA_NO_CONTAINER_FAILURE;
        }

        ContainerTask containerTask = processingContainer.getTasksCache().get(packet.getTaskID());

        if (containerTask == null) {
            this.logger.warning("No such task in container with containerId="
                            + packet.getContainerId()
                            + " taskId="
                            + packet.getTaskID()
                            + " jetPacket="
                            + packet
                            + ". Application will be interrupted."
            );

            return JetPacket.HEADER_JET_DATA_NO_TASK_FAILURE;
        }

        containerTask.getShufflingReceiver(this.jetAddress).consume(packet);
        return 0;
    }
}
