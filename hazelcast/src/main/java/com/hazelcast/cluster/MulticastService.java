/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cluster;

import com.hazelcast.config.Config;
import com.hazelcast.config.MulticastConfig;
import com.hazelcast.instance.Node;
import com.hazelcast.instance.OutOfMemoryErrorDispatcher;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.BufferObjectDataInput;
import com.hazelcast.nio.BufferObjectDataOutput;
import com.hazelcast.nio.Packet;
import com.hazelcast.nio.serialization.HazelcastSerializationException;

import java.io.EOFException;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.InetAddress;
import java.net.MulticastSocket;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class MulticastService implements Runnable {

    private static final int DATAGRAM_BUFFER_SIZE = 64 * 1024;

    private final ILogger logger;
    private final MulticastSocket multicastSocket;
    private final DatagramPacket datagramPacketSend;
    private final DatagramPacket datagramPacketReceive;
    private final Object sendLock = new Object();
    private final CountDownLatch stopLatch = new CountDownLatch(1);
    private final List<MulticastListener> listeners = new CopyOnWriteArrayList<MulticastListener>();
    private final Node node;

    private final BufferObjectDataOutput sendOutput;

    private volatile boolean running = true;

    public MulticastService(Node node, MulticastSocket multicastSocket) throws Exception {
        this.node = node;
        logger = node.getLogger(MulticastService.class.getName());
        Config config = node.getConfig();
        this.multicastSocket = multicastSocket;

        sendOutput = node.getSerializationService().createObjectDataOutput(1024);
        datagramPacketReceive = new DatagramPacket(new byte[DATAGRAM_BUFFER_SIZE], DATAGRAM_BUFFER_SIZE);
        final MulticastConfig multicastConfig = config.getNetworkConfig().getJoin().getMulticastConfig();
        datagramPacketSend = new DatagramPacket(new byte[0], 0, InetAddress
                .getByName(multicastConfig.getMulticastGroup()), multicastConfig.getMulticastPort());
        running = true;
    }

    public void addMulticastListener(MulticastListener multicastListener) {
        listeners.add(multicastListener);
    }

    public void removeMulticastListener(MulticastListener multicastListener) {
        listeners.remove(multicastListener);
    }

    public void stop() {
        try {
            if (!running && multicastSocket.isClosed()) {
                return;
            }
            try {
                multicastSocket.close();
            } catch (Throwable ignored) {
            }
            running = false;
            if (!stopLatch.await(5, TimeUnit.SECONDS)) {
                logger.warning("Failed to shutdown MulticastService in 5 seconds!");
            }
        } catch (Throwable e) {
            logger.warning(e);
        }
    }

    private void cleanup() {
        running = false;
        try {
            sendOutput.close();
            datagramPacketReceive.setData(new byte[0]);
            datagramPacketSend.setData(new byte[0]);
        } catch (Throwable ignored) {
        }
        stopLatch.countDown();
    }

    @SuppressWarnings("WhileLoopSpinsOnField")
    @Override
    public void run() {
        try {
            while (running) {
                try {
                    final JoinMessage joinMessage = receive();
                    if (joinMessage != null) {
                        for (MulticastListener multicastListener : listeners) {
                            try {
                                multicastListener.onMessage(joinMessage);
                            } catch (Exception e) {
                                logger.warning(e);
                            }
                        }
                    }
                } catch (OutOfMemoryError e) {
                    OutOfMemoryErrorDispatcher.onOutOfMemory(e);
                } catch (Exception e) {
                    logger.warning(e);
                }
            }
        } finally {
            cleanup();
        }
    }

    private JoinMessage receive() {
        try {
            try {
                multicastSocket.receive(datagramPacketReceive);
            } catch (IOException ignore) {
                return null;
            }
            try {
                final byte[] data = datagramPacketReceive.getData();
                final int offset = datagramPacketReceive.getOffset();
                final BufferObjectDataInput input = node.getSerializationService().createObjectDataInput(data);
                input.position(offset);

                final byte packetVersion = input.readByte();
                if (packetVersion != Packet.VERSION) {
                    logger.warning("Received a JoinRequest with a different packet version! This -> "
                            + Packet.VERSION + ", Incoming -> " + packetVersion
                            + ", Sender -> " + datagramPacketReceive.getAddress());
                    return null;
                }
                try {
                    return input.readObject();
                } finally {
                    input.close();
                }
            } catch (Exception e) {
                if (e instanceof EOFException || e instanceof HazelcastSerializationException) {
                    logger.warning("Received data format is invalid." +
                            " (An old version of Hazelcast may be running here.), Sender -> "
 +                                    + datagramPacketReceive.getAddress(), e);
                } else {
                    throw e;
                }
            }
        } catch (Exception e) {
            logger.warning(e);
        }
        return null;
    }

    public void send(JoinMessage joinMessage) {
        if (!running) return;
        final BufferObjectDataOutput out = sendOutput;
        synchronized (sendLock) {
            try {
                out.writeByte(Packet.VERSION);
                out.writeObject(joinMessage);
                datagramPacketSend.setData(out.toByteArray());
                multicastSocket.send(datagramPacketSend);
                out.clear();
            } catch (IOException e) {
                logger.warning("You probably have too long Hazelcast configuration!", e);
            }
        }
    }
}
