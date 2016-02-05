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


import java.util.List;
import java.util.Queue;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.io.IOException;

import com.hazelcast.nio.Address;

import java.nio.channels.Selector;
import java.net.InetSocketAddress;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;

import com.hazelcast.jet.api.executor.Payload;

import java.util.concurrent.ConcurrentLinkedQueue;

import com.hazelcast.jet.api.actor.ObjectProducer;
import com.hazelcast.jet.impl.hazelcast.JetPacket;
import com.hazelcast.jet.api.data.io.SocketWriter;
import com.hazelcast.jet.impl.actor.RingBufferActor;
import com.hazelcast.jet.api.application.ApplicationContext;

public class DefaultSocketWriter
        extends AbstractNetworkTask implements SocketWriter {
    private int lastFrameId = -1;

    private int nextProducerIdx;

    private JetPacket lastPacket;

    private int lastProducedCount;

    private Object[] currentFrames;

    private Selector selector;

    private final Address jetAddress;

    private final byte[] membersBytes;

    private final ByteBuffer sendByteBuffer;

    private final InetSocketAddress inetSocketAddress;

    private final ApplicationContext applicationContext;

    private final List<RingBufferActor> producers = new ArrayList<RingBufferActor>();

    private final Queue<JetPacket> servicePackets = new ConcurrentLinkedQueue<JetPacket>();

    public DefaultSocketWriter(ApplicationContext applicationContext,
                               Address jetAddress) {
        super(applicationContext.getNodeEngine());
        this.inetSocketAddress = new InetSocketAddress(jetAddress.getHost(), jetAddress.getPort());
        this.sendByteBuffer = ByteBuffer.allocateDirect(applicationContext.getJetApplicationConfig().getDefaultTCPBufferSize());
        this.applicationContext = applicationContext;
        this.jetAddress = jetAddress;
        this.membersBytes = applicationContext.getNodeEngine().getSerializationService().toBytes(
                applicationContext.getLocalJetAddress()
        );

        reset();
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

        if (processSocketChannel()) {
            return;
        }

        if (!writeToSocket(payload)) {
            return;
        }

        if (processLastPacket(payload)) {
            return;
        }

        processProducers(payload);
    }

    private boolean processLastPacket(Payload payload) {
        if (this.lastPacket != null) {
            if (!processPacket(this.lastPacket, payload)) {
                return true;
            }

            if (!writeToSocket(payload)) {
                return true;
            }
        }
        return false;
    }

    private boolean processSocketChannel() {
        if ((this.socketChannel == null) || (!this.socketChannel.isConnected())) {
            if (!connect()) {
                this.socketChannel = null;
                return true;
            } else {
                if (this.lastPacket == null) {
                    JetPacket packet = new JetPacket(this.applicationContext.getName().getBytes(), this.membersBytes);
                    packet.setHeader(JetPacket.HEADER_JET_MEMBER_EVENT);
                    this.lastPacket = packet;
                }
            }
        }
        return false;
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
            if (this.selector == null) {
                this.socketChannel.socket().getOutputStream().flush();
                this.selector = this.socketChannel.provider().openSelector();
                this.socketChannel.register(this.selector, SelectionKey.OP_WRITE);
            }

            if (this.selector.selectNow() > 0) {
                for (SelectionKey key : this.selector.keys()) {
                    if ((key.isWritable()) && (key.isValid())) {
                        this.selector.close();
                        this.finished = true;
                    }
                }
            }
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

    private boolean writeToSocket(Payload payload) {
        if (this.sendByteBuffer.position() > 0) {
            try {
                this.sendByteBuffer.flip();

                int bytesWritten = this.socketChannel.write(this.sendByteBuffer);

                payload.set(bytesWritten > 0);

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

    private boolean connect() {
        try {
            if (this.socketChannel != null) {
                this.socketChannel.close();
            }

            if (this.selector != null) {
                this.selector.close();
            }

            this.socketChannel = SocketChannel.open(this.inetSocketAddress);
            this.socketChannel.configureBlocking(false);
            return this.socketChannel.finishConnect();
        } catch (IOException e) {
            return false;
        }
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
