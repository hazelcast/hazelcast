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
import com.hazelcast.jet.runtime.Producer;
import com.hazelcast.jet.impl.data.io.JetPacket;
import com.hazelcast.jet.impl.job.JobContext;
import com.hazelcast.jet.impl.util.BooleanHolder;
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

/**
 * Represents task to write to network socket
 * <p/>
 * The architecture is following:
 * <p/>
 * <pre>
 *  Producer(ringBuffer)  -&gt; SocketWriter -&gt; SocketChannel (JetAddress)
 *  </pre>
 */
public class SocketWriter
        extends NetworkTask {
    private final byte[] membersBytes;
    private final ByteBuffer sendByteBuffer;
    private final byte[] jobNameBytes;
    private final InetSocketAddress inetSocketAddress;
    private final JobContext jobContext;
    private final List<Producer> producers = new ArrayList<Producer>();
    private final Queue<JetPacket> servicePackets = new ConcurrentLinkedQueue<JetPacket>();

    private int lastFrameId = -1;
    private int nextProducerIdx;
    private JetPacket lastPacket;
    private int lastProducedCount;
    private Object[] currentFrames;
    private boolean memberEventSent;

    public SocketWriter(JobContext jobContext,
                        Address jetAddress) {
        super(jobContext.getNodeEngine(), jetAddress);

        this.inetSocketAddress = new InetSocketAddress(jetAddress.getHost(), jetAddress.getPort());

        this.sendByteBuffer = ByteBuffer.allocateDirect(jobContext.getJobConfig().getTcpBufferSize())
                .order(ByteOrder.BIG_ENDIAN);

        this.jobContext = jobContext;

        InternalSerializationService serializationService = (InternalSerializationService) jobContext
                .getNodeEngine().getSerializationService();

        this.membersBytes = serializationService
                .toBytes(
                        jobContext.getLocalJetAddress()
                );

        this.jobNameBytes = serializationService.toBytes(
                jobContext.getName()
        );

        reset();
    }

    /**
     * Init task, perform initialization actions before task being executed
     * The strict rule is that this method will be executed synchronously on
     * all nodes in cluster before any real task's  execution
     */
    public void init() {
        super.init();
        memberEventSent = false;
    }

    private boolean checkServicesQueue(BooleanHolder payload) {
        if (lastFrameId < 0 && sendByteBuffer.position() == 0 && lastPacket == null) {
            JetPacket packet = servicePackets.poll();
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
        jobContext.getJobManager().notifyNetworkTaskFinished();
    }

    @Override
    public boolean onExecute(BooleanHolder payload) throws Exception {
        if (destroyed) {
            closeSocket();
            return false;
        }

        if (interrupted) {
            checkServicesQueue(payload);
            closeSocket();
            finalized = true;
            notifyAMTaskFinished();
            return false;
        }

        payload.set(false);
        process(payload);
        return checkFinished();
    }


    private void process(BooleanHolder payload) throws Exception {
        if (!checkServicesQueue(payload)) {
            return;
        }

        if (!processSocketChannel()) {
            return;
        }

        if (!memberEventSent) {
            JetPacket packet = new JetPacket(jobNameBytes, membersBytes);
            packet.setHeader(JetPacket.HEADER_JET_MEMBER_EVENT);
            lastPacket = packet;
            memberEventSent = true;
        }

        if (!writeToSocket(payload)) {
            return;
        }

        if (!processLastPacket(payload)) {
            return;
        }

        processProducers(payload);
    }

    private boolean processLastPacket(BooleanHolder payload) {
        if (lastPacket != null) {
            if (!processPacket(lastPacket, payload)) {
                return false;
            }

            if (!writeToSocket(payload)) {
                return false;
            }
        }

        return true;
    }

    private boolean processSocketChannel() throws IOException {
        if ((socketChannel != null) && (!socketChannel.finishConnect())) {
            return false;
        }

        if ((socketChannel == null) || (!socketChannel.isConnected())) {
            connect();
            return false;
        }

        return true;
    }

    private boolean processProducers(BooleanHolder payload) throws Exception {
        if (lastFrameId >= 0) {
            if (!processFrames(payload)) {
                return true;
            }
        } else {
            int startFrom = waitingForFinish ? 0 : nextProducerIdx;

            boolean activeProducer = false;

            for (int i = startFrom; i < producers.size(); i++) {
                Producer producer = producers.get(i);

                currentFrames = producer.produce();

                if (currentFrames == null) {
                    continue;
                }

                activeProducer = true;

                payload.set(true);

                lastFrameId = -1;
                lastProducedCount = producer.lastProducedCount();

                if (!processFrames(payload)) {
                    nextProducerIdx = (i + 1) % producers.size();
                    return true;
                }
            }

            checkTaskFinished(activeProducer);
            reset();
        }

        return false;
    }

    private void checkTaskFinished(boolean activeProducer) throws IOException {
        if ((!activeProducer) && (waitingForFinish)) {
            finished = true;
        }
    }

    private void reset() {
        lastFrameId = -1;
        lastPacket = null;
        nextProducerIdx = 0;
        currentFrames = null;
        lastProducedCount = 0;
    }

    private boolean processFrames(BooleanHolder payload) {
        for (int i = lastFrameId + 1; i < lastProducedCount; i++) {
            JetPacket packet = (JetPacket) currentFrames[i];

            if (!processPacket(packet, payload)) {
                lastPacket = packet;
                lastFrameId = i;
                return false;
            }

            if (!writeToSocket(payload)) {
                lastFrameId = i;
                return false;
            }
        }

        lastPacket = null;
        lastFrameId = -1;
        lastProducedCount = 0;
        return true;
    }

    private boolean processPacket(JetPacket packet, BooleanHolder payload) {
        if (!writePacket(packet)) {
            writeToSocket(payload);
            lastPacket = packet;
            return false;
        }

        lastPacket = null;
        return true;
    }

    private boolean writePacket(JetPacket packet) {
        return packet.writeTo(sendByteBuffer);
    }


    /**
     * Close network socket;
     */
    public void closeSocket() {
        if (socketChannel != null) {
            try {
                socketChannel.close();
                socketChannel = null;
            } catch (IOException e) {
                logger.warning(e.getMessage(), e);
            }
        }
    }


    private boolean writeToSocket(BooleanHolder payload) {
        if (sendByteBuffer.position() > 0) {
            try {
                sendByteBuffer.flip();

                if (socketChannel != null) {
                    int bytesWritten = socketChannel.write(sendByteBuffer);
                    payload.set(bytesWritten > 0);
                }

                if (sendByteBuffer.hasRemaining()) {
                    sendByteBuffer.compact();
                    return false;
                } else {
                    sendByteBuffer.clear();
                    return true;
                }
            } catch (IOException e) {
                closeSocket();
            }
        }

        return true;
    }

    private void connect() throws IOException {
        if (socketChannel != null) {
            socketChannel.close();
        }
        socketChannel = SocketChannel.open(inetSocketAddress);
        socketChannel.configureBlocking(false);
        //socketChannel.socket().setSendBufferSize(1);
    }

    public void registerProducer(Producer producer) {
        producers.add(producer);
    }

    /**
     * Sends service packet on output node - it will be processed in urgent mode;
     *
     * @param jetPacket - Jet network packet;
     */
    public void sendServicePacket(JetPacket jetPacket) {
        servicePackets.offer(jetPacket);
    }

}
