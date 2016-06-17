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

package com.hazelcast.jet.impl.container.task.nio;


import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.jet.impl.actor.ObjectProducer;
import com.hazelcast.jet.impl.data.io.SocketWriter;
import com.hazelcast.jet.impl.application.ApplicationContext;
import com.hazelcast.jet.impl.executor.Payload;
import com.hazelcast.jet.impl.actor.RingBufferActor;
import com.hazelcast.jet.impl.hazelcast.JetPacket;
import com.hazelcast.nio.Address;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

public class DefaultSocketWriter
        extends AbstractNetworkTask implements SocketWriter {
    private final byte[] membersBytes;
    private final ByteBuffer sendByteBuffer;
    private final byte[] applicationNameBytes;
    private final InetSocketAddress inetSocketAddress;
    private final ApplicationContext applicationContext;
    private final List<RingBufferActor> producers = new ArrayList<RingBufferActor>();
    private final Queue<JetPacket> servicePackets = new ConcurrentLinkedQueue<JetPacket>();

    private int lastFrameId = -1;
    private int nextProducerIdx;
    private JetPacket lastPacket;
    private int lastProducedCount;
    private Object[] currentFrames;
    private boolean memberEventSent;

    public DefaultSocketWriter(ApplicationContext applicationContext,
                               Address jetAddress) {
        super(applicationContext.getNodeEngine(), jetAddress);

        this.inetSocketAddress = new InetSocketAddress(jetAddress.getHost(), jetAddress.getPort());

        this.sendByteBuffer = ByteBuffer.allocateDirect(applicationContext.getApplicationConfig().getTcpBufferSize())
                .order(ByteOrder.BIG_ENDIAN);

        this.applicationContext = applicationContext;

        InternalSerializationService serializationService = (InternalSerializationService) applicationContext
                .getNodeEngine().getSerializationService();

        this.membersBytes = serializationService
                .toBytes(
                applicationContext.getLocalJetAddress()
        );

        this.applicationNameBytes = serializationService.toBytes(
                applicationContext.getName()
        );

        reset();
    }

    public void init() {
        super.init();
        this.memberEventSent = false;
    }

    private boolean checkServicesQueue(Payload payload) {
        if (
                (this.lastFrameId < 0)
                        &&
                        (this.sendByteBuffer.position() == 0)
                        &&
                        (this.lastPacket == null)
                ) {
            JetPacket packet = this.servicePackets.poll();

            if (packet != null) {
                if (!processPacket(packet, payload)) {
                    return false;
                }

                writeToSocket(payload);
            }
        }

        return true;
    }

    @Override
    protected void notifyAMTaskFinished() {
        this.applicationContext.getApplicationMaster().notifyNetworkTaskFinished();
    }

    @Override
    public boolean onExecute(Payload payload) throws Exception {
        if (this.destroyed) {
            closeSocket();
            return false;
        }

        if (this.interrupted) {
            checkServicesQueue(payload);
            closeSocket();
            this.finalized = true;
            notifyAMTaskFinished();
            return false;
        }

        payload.set(false);
        process(payload);
        return checkFinished();
    }


    private void process(Payload payload) throws Exception {
        if (!checkServicesQueue(payload)) {
            return;
        }

        if (!processSocketChannel()) {
            return;
        }

        if (!this.memberEventSent) {
            JetPacket packet = new JetPacket(this.applicationNameBytes, this.membersBytes);
            packet.setHeader(JetPacket.HEADER_JET_MEMBER_EVENT);
            this.lastPacket = packet;
            this.memberEventSent = true;
        }

        if (!writeToSocket(payload)) {
            return;
        }

        if (!processLastPacket(payload)) {
            return;
        }

        processProducers(payload);
    }

    private boolean processLastPacket(Payload payload) {
        if (this.lastPacket != null) {
            if (!processPacket(this.lastPacket, payload)) {
                return false;
            }

            if (!writeToSocket(payload)) {
                return false;
            }
        }

        return true;
    }

    private boolean processSocketChannel() throws IOException {
        if ((this.socketChannel != null) && (!this.socketChannel.finishConnect())) {
            return false;
        }

        if ((this.socketChannel == null) || (!this.socketChannel.isConnected())) {
            connect();
            return false;
        }

        return true;
    }

    private boolean processProducers(Payload payload) throws Exception {
        if (this.lastFrameId >= 0) {
            if (!processFrames(payload)) {
                return true;
            }
        } else {
            int startFrom = this.waitingForFinish ? 0 : this.nextProducerIdx;

            boolean activeProducer = false;

            for (int i = startFrom; i < this.producers.size(); i++) {
                ObjectProducer producer = this.producers.get(i);

                this.currentFrames = producer.produce();

                if (this.currentFrames == null) {
                    continue;
                }

                activeProducer = true;

                payload.set(true);

                this.lastFrameId = -1;
                this.lastProducedCount = producer.lastProducedCount();

                if (!processFrames(payload)) {
                    this.nextProducerIdx = (i + 1) % this.producers.size();
                    return true;
                }
            }

            checkTaskFinished(activeProducer);
            reset();
        }

        return false;
    }

    private void checkTaskFinished(boolean activeProducer) throws IOException {
        if ((!activeProducer) && (this.waitingForFinish)) {
            this.finished = true;
        }
    }

    private void reset() {
        this.lastFrameId = -1;
        this.lastPacket = null;
        this.nextProducerIdx = 0;
        this.currentFrames = null;
        this.lastProducedCount = 0;
    }

    private boolean processFrames(Payload payload) {
        for (int i = this.lastFrameId + 1; i < this.lastProducedCount; i++) {
            JetPacket packet = (JetPacket) this.currentFrames[i];

            if (!processPacket(packet, payload)) {
                this.lastPacket = packet;
                this.lastFrameId = i;
                return false;
            }

            if (!writeToSocket(payload)) {
                this.lastFrameId = i;
                return false;
            }
        }

        this.lastPacket = null;
        this.lastFrameId = -1;
        this.lastProducedCount = 0;
        return true;
    }

    private boolean processPacket(JetPacket packet, Payload payload) {
        if (!writePacket(packet)) {
            writeToSocket(payload);
            this.lastPacket = packet;
            return false;
        }

        this.lastPacket = null;
        return true;
    }

    private boolean writePacket(JetPacket packet) {
        return packet.writeTo(this.sendByteBuffer);
    }


    @Override
    public void closeSocket() {
        if (this.socketChannel != null) {
            try {
                this.socketChannel.close();
                this.socketChannel = null;
            } catch (IOException e) {
                this.logger.warning(e.getMessage(), e);
            }
        }
    }


    private boolean writeToSocket(Payload payload) {
        if (this.sendByteBuffer.position() > 0) {
            try {
                this.sendByteBuffer.flip();

                SocketChannel socketChannel = this.socketChannel;

                if (socketChannel != null) {
                    int bytesWritten = socketChannel.write(this.sendByteBuffer);
                    payload.set(bytesWritten > 0);
                }

                if (this.sendByteBuffer.hasRemaining()) {
                    this.sendByteBuffer.compact();
                    return false;
                } else {
                    this.sendByteBuffer.clear();
                    return true;
                }
            } catch (IOException e) {
                closeSocket();
            }
        }

        return true;
    }

    private void connect() throws IOException {
        if (this.socketChannel != null) {
            this.socketChannel.close();
        }
        this.socketChannel = SocketChannel.open(this.inetSocketAddress);
        this.socketChannel.configureBlocking(false);
        //this.socketChannel.socket().setSendBufferSize(1);
    }

    @Override
    public void registerProducer(RingBufferActor ringBufferActor) {
        this.producers.add(ringBufferActor);
    }

    @Override
    public void sendServicePacket(JetPacket jetPacket) {
        this.servicePackets.offer(jetPacket);
    }
}
